package projection

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"

	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
)

type checkpoint struct {
	EventID  eventid.EventID
	Sequence uint64
}

type Checkpoints[K eventsourcing.ID] struct {
	store store.KVStore
}

func NewCheckpoints[K eventsourcing.ID](store store.KVStore) *Checkpoints[K] {
	return &Checkpoints[K]{
		store: store,
	}
}

// Reject verifies if projection should accept the event within the message.
//
// When the message if of type MessageKindSwitch, it signals that the catchup has been done and that from now on all events will come from the event bus
func (p *Checkpoints[K]) Reject(ctx context.Context, msg Message[K]) (bool, error) {
	meta := msg.Meta

	if meta.Kind == MessageKindSwitch {
		err := p.put(ctx, meta.Name, checkpoint{EventID: msg.Message.ID})
		if err != nil {
			return false, faults.Wrap(err)
		}

		return true, nil
	}

	cp, err := p.get(ctx, fmt.Sprintf("%s-%d", meta.Name, meta.Partition))
	if err != nil {
		return false, faults.Wrap(err)
	}

	if meta.Kind == MessageKindCatchup {
		return msg.Message.ID.Compare(cp.EventID) <= 0, nil
	}

	if (cp == checkpoint{}) {
		cp2, err := p.get(ctx, meta.Name)
		if err != nil {
			return false, faults.Wrap(err)
		}
		return msg.Message.ID.Compare(cp2.EventID) <= 0, nil
	}

	if msg.Message.ID.Compare(cp.EventID) <= 0 {
		return true, nil
	}

	return msg.Meta.Sequence <= cp.Sequence, nil
}

// Save saves the checkpoint for the projection.
//
// The context must contain a database transaction.
func (p *Checkpoints[K]) Save(ctx context.Context, msg Message[K]) error {
	meta := msg.Meta

	switch meta.Kind {
	case MessageKindCatchup:
		err := p.put(ctx, fmt.Sprintf("%s-%d", meta.Name, meta.Partition), checkpoint{EventID: msg.Message.ID})
		if err != nil {
			return faults.Wrap(err)
		}

	case MessageKindLive:
		cp, err := p.get(ctx, meta.Name)
		if err != nil {
			return faults.Wrap(err)
		}

		err = p.put(ctx, fmt.Sprintf("%s-%d", meta.Name, meta.Partition), checkpoint{
			// we save the last event ID from the catchup since it can be used be used to reject the first non catchup event
			EventID:  cp.EventID,
			Sequence: meta.Sequence,
		})
		if err != nil {
			return faults.Wrap(err)
		}

	default:
		return faults.Errorf("unexpected message kind: '%s'", meta.Kind)
	}

	return nil
}

func (p *Checkpoints[K]) get(ctx context.Context, name string) (checkpoint, error) {
	v, err := p.store.Get(ctx, name)
	if err != nil {
		return checkpoint{}, faults.Wrap(err)
	}

	cp := checkpoint{}
	if v == "" {
		return cp, nil
	}

	err = json.Unmarshal([]byte(v), &cp)
	if err != nil {
		return checkpoint{}, faults.Wrap(err)
	}
	return cp, nil
}

func (p *Checkpoints[K]) put(ctx context.Context, name string, value checkpoint) error {
	b, err := json.Marshal(value)
	if err != nil {
		return faults.Wrap(err)
	}
	err = p.store.Put(ctx, name, string(b))
	return faults.Wrap(err)
}
