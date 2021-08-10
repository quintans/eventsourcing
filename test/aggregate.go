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

type AccountCreated struct {
	ID    uuid.UUID `json:"id,omitempty"`
	Money int64     `json:"money,omitempty"`
	Owner string    `json:"owner,omitempty"`
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

type Factory struct {
	AggregateFactory
	EventFactory
}

type AggregateFactory struct{}

func (f AggregateFactory) NewAggregate(kind eventsourcing.AggregateType) (eventsourcing.Aggregater, error) {
	switch kind {
	case "Account":
		return NewAccount(), nil
	default:
		return nil, faults.Errorf("unknown aggregate type: %s", kind)
	}
}

type EventFactory struct{}

func (EventFactory) NewEvent(kind eventsourcing.EventKind) (eventsourcing.Typer, error) {
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
	a := &Account{}
	a.RootAggregate = eventsourcing.NewRootAggregate(a)
	a.ID = id
	a.ApplyChange(AccountCreated{
		ID:    id,
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
	ID      uuid.UUID `json:"id"`
	Status  Status    `json:"status,omitempty"`
	Balance int64     `json:"balance,omitempty"`
	Owner   string    `json:"owner,omitempty"`
}

func (a Account) GetID() string {
	return a.ID.String()
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
	a.ID = event.ID
	a.Balance = event.Money
	a.Owner = event.Owner
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
