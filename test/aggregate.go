package test

import (
	"github.com/google/uuid"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
)

type Status string

const (
	OPEN   Status = "OPEN"
	CLOSED Status = "CLOSED"
	FROZEN Status = "FROZEN"
)

type MyUUID uuid.UUID

// MarshalJSON returns m as a base64 encoding of m.
func (m MyUUID) MarshalJSON() ([]byte, error) {
	u := uuid.UUID(m)
	if u == uuid.Nil {
		return []byte{}, nil
	}
	encoded := `"` + u.String() + `"`
	return []byte(encoded), nil
}

// UnmarshalJSON sets *m to a decoded base64.
func (m *MyUUID) UnmarshalJSON(data []byte) error {
	if m == nil {
		return faults.New("test.MyUUID: UnmarshalJSON on nil pointer")
	}
	// strip quotes
	data = data[1 : len(data)-1]

	decoded, err := uuid.Parse(string(data))
	if err != nil {
		return faults.Errorf("test.MyUUID: decode error: %w", err)
	}

	*m = MyUUID(decoded)
	return nil
}

type AccountCreated struct {
	ID    MyUUID `json:"id,omitempty"`
	Money int64  `json:"money,omitempty"`
	Owner string `json:"owner,omitempty"`
}

func (e AccountCreated) GetType() string {
	return "AccountCreated"
}

type MoneyWithdrawn struct {
	Money int64 `json:"money,omitempty"`
}

func (e MoneyWithdrawn) GetType() string {
	return "MoneyWithdrawn"
}

type MoneyDeposited struct {
	Money int64 `json:"money,omitempty"`
}

func (e MoneyDeposited) GetType() string {
	return "MoneyDeposited"
}

type OwnerUpdated struct {
	Owner string `json:"owner,omitempty"`
}

func (e OwnerUpdated) GetType() string {
	return "OwnerUpdated"
}

type AggregateFactory struct {
	EventFactory
}

func (f AggregateFactory) New(kind string) (eventsourcing.Typer, error) {
	switch kind {
	case "Account":
		return NewAccount(), nil
	default:
		evt, err := f.EventFactory.New(kind)
		if err != nil {
			return nil, err
		}
		return evt, nil
	}
}

type EventFactory struct{}

func (EventFactory) New(kind string) (eventsourcing.Typer, error) {
	var e eventsourcing.Typer
	switch kind {
	case "AccountCreated":
		e = &AccountCreated{}
	case "MoneyDeposited":
		e = &MoneyDeposited{}
	case "MoneyWithdrawn":
		e = &MoneyWithdrawn{}
	case "OwnerUpdated":
		e = &OwnerUpdated{}
	}
	if e == nil {
		return nil, faults.Errorf("Unknown event kind: %s", kind)
	}
	return e, nil
}

func CreateAccount(owner string, id uuid.UUID, money int64) *Account {
	a := &Account{
		Status:  OPEN,
		Balance: money,
		Owner:   owner,
	}
	a.RootAggregate = eventsourcing.NewRootAggregate(a)
	a.RootAggregate.ID = id
	a.ApplyChange(AccountCreated{
		ID:    MyUUID(id),
		Money: money,
		Owner: owner,
	})
	return a
}

func NewAccount() *Account {
	a := &Account{}
	a.RootAggregate = eventsourcing.NewRootAggregate(a)
	return a
}

type Account struct {
	eventsourcing.RootAggregate
	Status  Status `json:"status,omitempty"`
	Balance int64  `json:"balance,omitempty"`
	Owner   string `json:"owner,omitempty"`
}

func (a Account) GetType() string {
	return "Account"
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

func (a *Account) HandleEvent(event eventsourcing.Eventer) {
	switch t := event.(type) {
	case AccountCreated:
		a.HandleAccountCreated(t)
	case MoneyDeposited:
		a.HandleMoneyDeposited(t)
	case MoneyWithdrawn:
		a.HandleMoneyWithdrawn(t)
	case OwnerUpdated:
		a.HandleOwnerUpdated(t)
	}
}

func (a *Account) HandleAccountCreated(event AccountCreated) {
	a.ID = uuid.UUID(event.ID)
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

func ApplyChangeFromHistory(es eventsourcing.EventStore, agg eventsourcing.Aggregater, e eventsourcing.Event) error {
	m := eventsourcing.EventMetadata{
		AggregateVersion: e.AggregateVersion,
		CreatedAt:        e.CreatedAt,
	}
	evt, err := es.RehydrateEvent(e.Kind, e.Body)
	if err != nil {
		return err
	}
	agg.ApplyChangeFromHistory(m, evt)

	return nil
}
