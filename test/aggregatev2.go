package test

import (
	"github.com/google/uuid"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
)

// This represent the new software version after a migration
// Using the suffix V2 should only be used by the migrate events. Here we use everywhere to be easy to test.

type AccountCreatedV2 struct {
	ID        uuid.UUID `json:"id,omitempty"`
	Money     int64     `json:"money,omitempty"`
	FirstName string    `json:"first_name,omitempty"`
	LastName  string    `json:"last_name,omitempty"`
}

func (e AccountCreatedV2) GetType() string {
	return "AccountCreated_V2"
}

type OwnerUpdatedV2 struct {
	FirstName string `json:"first_name,omitempty"`
	LastName  string `json:"last_name,omitempty"`
}

func (e OwnerUpdatedV2) GetType() string {
	return "OwnerUpdated_V2"
}

type FactoryV2 struct {
	AggregateFactoryV2
	EventFactoryV2
}

type AggregateFactoryV2 struct{}

func (f AggregateFactoryV2) NewAggregate(kind eventsourcing.AggregateType) (eventsourcing.Aggregater, error) {
	switch kind {
	case "Account":
		return NewAccountV2(), nil
	default:
		return nil, faults.Errorf("unknown aggregate type: %s", kind)
	}
}

type EventFactoryV2 struct{}

func (EventFactoryV2) NewEvent(kind eventsourcing.EventKind) (eventsourcing.Typer, error) {
	var e eventsourcing.Typer
	switch kind {
	case "AccountCreated_V2":
		e = &AccountCreatedV2{}
	case "MoneyDeposited":
		e = &MoneyDeposited{}
	case "MoneyWithdrawn":
		e = &MoneyWithdrawn{}
	case "OwnerUpdated_V2":
		e = &OwnerUpdatedV2{}
	}
	if e == nil {
		return nil, faults.Errorf("Unknown event kind: %s", kind)
	}
	return e, nil
}

func CreateAccountV2(firstName string, lastName string, id uuid.UUID, money int64) *AccountV2 {
	a := &AccountV2{}
	a.RootAggregate = eventsourcing.NewRootAggregate(a)
	a.ID = id
	a.ApplyChange(AccountCreatedV2{
		ID:        id,
		Money:     money,
		FirstName: firstName,
		LastName:  lastName,
	})
	return a
}

func NewAccountV2() *AccountV2 {
	a := &AccountV2{}
	a.RootAggregate = eventsourcing.NewRootAggregate(a)
	return a
}

type AccountV2 struct {
	eventsourcing.RootAggregate
	ID        uuid.UUID `json:"id"`
	Status    Status    `json:"status,omitempty"`
	Balance   int64     `json:"balance,omitempty"`
	FirstName string    `json:"first_name,omitempty"`
	LastName  string    `json:"last_name,omitempty"`
}

func (a AccountV2) GetID() string {
	return a.ID.String()
}

func (a AccountV2) GetType() string {
	return "Account"
}

func (a *AccountV2) Withdraw(money int64) bool {
	if a.Balance >= money {
		a.ApplyChange(MoneyWithdrawn{Money: money})
		return true
	}
	return false
}

func (a *AccountV2) Deposit(money int64) {
	a.ApplyChange(MoneyDeposited{Money: money})
}

func (a *AccountV2) UpdateOwner(owner string) {
	a.ApplyChange(OwnerUpdated{Owner: owner})
}

func (a *AccountV2) HandleEvent(event eventsourcing.Eventer) {
	switch t := event.(type) {
	case AccountCreatedV2:
		a.HandleAccountCreatedV2(t)
	case MoneyDeposited:
		a.HandleMoneyDeposited(t)
	case MoneyWithdrawn:
		a.HandleMoneyWithdrawn(t)
	case OwnerUpdatedV2:
		a.HandleOwnerUpdatedV2(t)
	}
}

func (a *AccountV2) HandleAccountCreatedV2(event AccountCreatedV2) {
	a.ID = event.ID
	a.Balance = event.Money
	a.FirstName = event.FirstName
	a.LastName = event.LastName
	// this reflects that we are handling domain events and NOT property events
	a.Status = OPEN
}

func (a *AccountV2) HandleMoneyDeposited(event MoneyDeposited) {
	a.Balance += event.Money
}

func (a *AccountV2) HandleMoneyWithdrawn(event MoneyWithdrawn) {
	a.Balance -= event.Money
}

func (a *AccountV2) HandleOwnerUpdatedV2(event OwnerUpdatedV2) {
	a.FirstName = event.FirstName
	a.LastName = event.LastName
}
