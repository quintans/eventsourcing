package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/quintans/eventsourcing/log"
)

var turbo = time.Second

type Peer struct {
	Name    string
	Workers []string
}

// Ledger represents a member of a cluster of ledger to which a worker may be assigned
type Ledger interface {
	Name() string
	// Peers lists all ledgers/servers and the workers under each of them
	Peers(context.Context) ([]Peer, error)
	// Register registers the workers under this server
	Register(context.Context, []string) error
	// Close removes itself from the list
	Close(context.Context) error
}

// Worker represents an execution that can be started or stopped
type Worker interface {
	Name() string
	Group() string
	IsRunning() bool
	IsBalanceable() bool
	Start(context.Context) (bool, error)
	Stop(ctx context.Context)
}

type Balancer interface {
	Name() string
	Start(ctx context.Context) <-chan struct{}
	Stop(ctx context.Context)
}

var _ Balancer = (*WorkBalancer)(nil)

type MultiBalancerOption func(*WorkBalancer)

// WorkBalancer manages the lifecycle of workers and balance them over all peers.
//
// A worker can be unbalanced or paused.
//   - Unbalanced worker will never be balanced across the different peers and will always run.
//   - Paused worker will hold its execution and will not be balanced-
type WorkBalancer struct {
	name             string
	logger           *slog.Logger
	ledger           Ledger
	workers          []Worker
	heartbeat        time.Duration
	currentHeartbeat time.Duration
	turbo            time.Duration

	mu   sync.Mutex
	done chan struct{}
}

func NewWorkBalancer(logger *slog.Logger, name string, ledger Ledger, workers []Worker, options ...MultiBalancerOption) *WorkBalancer {
	mb := &WorkBalancer{
		name: name,
		logger: logger.With(
			"name", name,
		),
		ledger:           ledger,
		workers:          workers,
		heartbeat:        5 * time.Second,
		currentHeartbeat: 5 * time.Second,
		turbo:            turbo,
	}
	for _, o := range options {
		o(mb)
	}
	return mb
}

func WithHeartbeat(heartbeat time.Duration) MultiBalancerOption {
	return func(b *WorkBalancer) {
		b.heartbeat = heartbeat
		b.currentHeartbeat = heartbeat
	}
}

func WithTurboHeartbeat(heartbeat time.Duration) MultiBalancerOption {
	return func(b *WorkBalancer) {
		b.turbo = heartbeat
	}
}

func (b *WorkBalancer) Name() string {
	return b.name
}

func (b *WorkBalancer) Start(ctx context.Context) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.done != nil {
		return b.done
	}

	done := make(chan struct{})
	b.done = done
	go func() {
		ticker := time.NewTicker(b.heartbeat)
		defer ticker.Stop()
		for {
			err := b.run(ctx, ticker)
			if err != nil {
				b.logger.Warn("Error while balancing partitions", log.Err(err))
			}
			select {
			case <-done:
				return
			case <-ctx.Done():
				// shutdown
				b.Stop(context.Background())
				return
			case <-ticker.C:
			}
		}
	}()

	return done
}

func (b *WorkBalancer) Stop(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done == nil {
		return
	}

	for _, w := range b.workers {
		w.Stop(ctx)
	}

	err := b.ledger.Close(context.Background())
	if err != nil {
		b.logger.Warn("Error while cleaning register", log.Err(err))
	}

	close(b.done)
	b.done = nil
}

func (b *WorkBalancer) run(ctx context.Context, ticker *time.Ticker) error {
	peers, err := b.ledger.Peers(ctx)
	if err != nil {
		return err
	}
	balancedWorkers := b.filterBalancedWorkers()
	peers = filterPeerWorkers(peers, balancedWorkers)

	// if current peer is not in the list, add it to the peer count
	present := false
	for _, v := range peers {
		if v.Name == b.ledger.Name() {
			present = true
			break
		}
	}
	peersCount := len(peers)
	if !present {
		peersCount++
	}

	monitorsNo := len(balancedWorkers)
	minWorkersToAcquire := monitorsNo / peersCount

	// check if all peers have the minimum workers. Only after that, any additional can be picked up.
	allHaveMinWorkers := true
	workersInUse := map[string]bool{}
	for _, m := range peers {
		// checking if others have min required workers running.
		// This peer might be included
		if len(m.Workers) < minWorkersToAcquire {
			allHaveMinWorkers = false
		}
		// map only other peers workers
		if m.Name != b.ledger.Name() {
			for _, v := range m.Workers {
				workersInUse[v] = true
			}
		}
	}
	// mapping my current workers
	myRunningWorkers := map[string]bool{}
	for _, v := range balancedWorkers {
		if v.IsRunning() {
			workersInUse[v.Name()] = true
			myRunningWorkers[v.Name()] = true
		}
	}
	// if my current running workers are less, then not all peers have the min workers
	if len(myRunningWorkers) < minWorkersToAcquire {
		allHaveMinWorkers = false
	}

	if allHaveMinWorkers && monitorsNo%peersCount != 0 {
		minWorkersToAcquire++
	}

	// make sure that all unpaused and unbalanced workers are running
	for _, w := range b.workers {
		if !w.IsBalanceable() {
			_, err := w.Start(ctx)
			if err != nil {
				b.logger.Error("Failed to start worker", log.Err(err))
			}
		}
	}

	locks := b.balance(ctx, balancedWorkers, minWorkersToAcquire, workersInUse, myRunningWorkers)

	// reset ticker if needed
	if b.enableTurbo(balancedWorkers, peers, locks, minWorkersToAcquire) {
		// turbo is activated when there is a need to rebalance workers
		if b.currentHeartbeat != b.turbo {
			b.currentHeartbeat = b.turbo
			ticker.Reset(b.turbo)
		}
	} else {
		if b.currentHeartbeat != b.heartbeat {
			b.currentHeartbeat = b.heartbeat
			ticker.Reset(b.heartbeat)
		}
	}

	return b.ledger.Register(ctx, locks)
}

func (b *WorkBalancer) enableTurbo(workers []Worker, peers []Peer, myWorkers []string, minWorkersToAcquire int) bool {
	total := len(workers)
	acquired := len(myWorkers)
	if acquired < minWorkersToAcquire {
		return true
	}
	for _, m := range peers {
		if m.Name != b.ledger.Name() {
			if len(m.Workers) < minWorkersToAcquire {
				return true
			}
			acquired += len(m.Workers)
		}
	}
	return total != acquired
}

func (b *WorkBalancer) filterBalancedWorkers() []Worker {
	var balanceable []Worker
	for _, w := range b.workers {
		if w.IsBalanceable() {
			balanceable = append(balanceable, w)
		}
	}
	return balanceable
}

func filterPeerWorkers(peers []Peer, workers []Worker) []Peer {
	for _, m := range peers {
		newWorkers := []string{}
		for _, v := range m.Workers {
			for _, w := range workers {
				if v == w.Name() {
					newWorkers = append(newWorkers, v)
					break
				}
			}
		}
		m.Workers = newWorkers
	}
	return peers
}

func (b *WorkBalancer) balance(ctx context.Context, workers []Worker, workersToAcquire int, workersInUse, myRunningWorkers map[string]bool) []string {
	running := len(myRunningWorkers)
	if running == workersToAcquire {
		return mapToString(myRunningWorkers)
	}

	for _, w := range workers {
		if running > workersToAcquire {
			if !w.IsRunning() {
				continue
			}

			w.Stop(ctx)
			delete(myRunningWorkers, w.Name())
			running--
		} else {
			if workersInUse[w.Name()] {
				continue
			}

			ok, err := w.Start(ctx)
			if err != nil {
				b.logger.Error("Failed to start worker", log.Err(err))
			}

			if ok {
				myRunningWorkers[w.Name()] = true
				running++
			}
		}
		if running == workersToAcquire {
			break
		}
	}
	return mapToString(myRunningWorkers)
}

func mapToString(m map[string]bool) []string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	return s
}

var _ Balancer = (*SingleBalancer)(nil)

type SingleBalancer struct {
	logger    *slog.Logger
	name      string
	worker    Worker
	heartbeat time.Duration

	mu   sync.Mutex
	done chan struct{}
}

func RunSingleBalancer(ctx context.Context, logger *slog.Logger, worker Worker, heartbeat time.Duration) *SingleBalancer {
	balancer := &SingleBalancer{
		logger:    logger,
		name:      worker.Group(),
		worker:    worker,
		heartbeat: heartbeat,
	}
	go func() {
		balancer.Start(ctx)
		<-ctx.Done()
	}()

	return balancer
}

func NewSingleBalancer(logger *slog.Logger, worker Worker, heartbeat time.Duration) *SingleBalancer {
	return &SingleBalancer{
		logger:    logger,
		name:      worker.Group(),
		worker:    worker,
		heartbeat: heartbeat,
	}
}

func (b *SingleBalancer) Name() string {
	return b.name
}

func (b *SingleBalancer) Start(ctx context.Context) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.done != nil {
		return b.done
	}

	done := make(chan struct{})
	b.done = done
	go func() {
		ticker := time.NewTicker(b.heartbeat)
		defer ticker.Stop()
		for {
			_, err := b.worker.Start(ctx)
			if err != nil {
				b.logger.Error("Failed to start worker", log.Err(err))
			}

			select {
			case <-done:
				return
			case <-ctx.Done():
				// shutdown
				b.Stop(context.Background())
				return
			case <-ticker.C:
			}
		}
	}()

	return done
}

func (b *SingleBalancer) Stop(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done == nil {
		return
	}

	b.worker.Stop(ctx)

	close(b.done)
	b.done = nil
}
