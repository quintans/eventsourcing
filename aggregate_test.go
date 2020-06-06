package eventstore

import (
	"encoding/json"
	"reflect"
	"strings"
)

var methodHandlerPrefix = "Handle"

type Handler struct {
	eventType reflect.Type
	handle    func(event interface{})
}

// HandlersCache is a map of types to functions that will be used to route event sourcing events
type HandlersCache map[string]Handler

func NewRootAggregate(aggregate interface{}, id string, version int) RootAggregate {
	return RootAggregate{
		ID:       id,
		Version:  version,
		events:   []interface{}{},
		handlers: createHandlersCache(aggregate),
	}
}

func createHandlersCache(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	sourceValue := reflect.ValueOf(source)
	handlers := make(HandlersCache)

	methodCount := sourceType.NumMethod()
	for i := 0; i < methodCount; i++ {
		method := sourceType.Method(i)

		if strings.HasPrefix(method.Name, methodHandlerPrefix) {
			//   func (source *MySource) HandleMyEvent(e MyEvent).
			if method.Type.NumIn() == 2 {
				eventType := method.Type.In(1)
				handler := func(event interface{}) {
					eventValue := reflect.ValueOf(event)
					method.Func.Call([]reflect.Value{sourceValue, eventValue})
				}

				handlers[eventType.Name()] = Handler{
					eventType: eventType,
					handle:    handler,
				}
			}
		}
	}

	return handlers
}

type RootAggregate struct {
	ID      string `json:"id,omitempty"`
	Version int    `json:"version,omitempty"`

	events   []interface{}
	handlers HandlersCache
}

func (a RootAggregate) GetID() string {
	return a.ID
}

func (a RootAggregate) GetVersion() int {
	return a.Version
}

func (a *RootAggregate) SetVersion(version int) {
	a.Version = version
}

func (a RootAggregate) GetEvents() []interface{} {
	return a.events
}

func (a *RootAggregate) ClearEvents() {
	a.events = []interface{}{}
}

func (a *RootAggregate) ApplyChangeFromHistory(event Event) error {
	h, ok := a.handlers[event.Kind]
	if ok {
		evtPtr := reflect.New(h.eventType)
		if err := json.Unmarshal(event.Body, evtPtr.Interface()); err != nil {
			return err
		}
		h.handle(evtPtr.Elem().Interface())
	}
	a.Version = event.AggregateVersion
	return nil
}

func (a *RootAggregate) ApplyChange(event interface{}) {
	h, ok := a.handlers[nameFor(event)]
	if ok {
		h.handle(event)
	}
	a.events = append(a.events, event)
}

type Status string

const (
	OPEN   Status = "OPEN"
	CLOSED Status = "CLOSED"
	FROZEN Status = "FROZEN"
)

type AccountCreated struct {
	ID    string `json:"id,omitempty"`
	Money int64  `json:"money,omitempty"`
	Owner string `json:"owner,omitempty"`
}

type MoneyWithdrawn struct {
	Money int64 `json:"money,omitempty"`
}

type MoneyDeposited struct {
	Money int64 `json:"money,omitempty"`
}

type OwnerUpdated struct {
	Owner string `json:"owner,omitempty"`
}

func CreateAccount(owner string, id string, money int64) *Account {
	a := &Account{
		Status:  OPEN,
		Balance: money,
		Owner:   owner,
	}
	a.RootAggregate = NewRootAggregate(a, id, 0)
	a.events = []interface{}{
		AccountCreated{
			ID:    id,
			Money: money,
			Owner: owner,
		},
	}
	return a
}

func NewAccount() *Account {
	a := &Account{}
	a.RootAggregate = NewRootAggregate(a, "", 0)
	return a
}

type Account struct {
	RootAggregate
	Status  Status `json:"status,omitempty"`
	Balance int64  `json:"balance,omitempty"`
	Owner   string `json:"owner,omitempty"`
}

func (a *Account) HandleAccountCreated(event AccountCreated) {
	a.ID = event.ID
	a.Balance = event.Money
	// this reflects that we are handling domain events and NOT property events
	a.Status = OPEN
}

func (a *Account) HandleMoneyDeposited(event MoneyDeposited) {
	a.Balance += event.Money
}

func (a *Account) HandleMoneyWithdrawn(event MoneyWithdrawn) {
	a.Balance -= event.Money
}

func (a *Account) HandleOwnerUpdated(event OwnerUpdated) {
	a.Owner = event.Owner
}

func (a *Account) Withdraw(money int64) bool {
	if a.Balance >= money {
		a.ApplyChange(MoneyWithdrawn{Money: money})
		return true
	}
	return false
}

func (a *Account) Deposit(money int64) {
	a.ApplyChange(MoneyDeposited{Money: money})
}

func (a *Account) UpdateOwner(owner string) {
	a.ApplyChange(OwnerUpdated{Owner: owner})
}
