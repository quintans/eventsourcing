package test

import (
	"github.com/quintans/eventstore"
	"github.com/quintans/faults"
)

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

func (_ AccountCreated) GetType() string {
	return "AccountCreated"
}

type MoneyWithdrawn struct {
	Money int64 `json:"money,omitempty"`
}

func (_ MoneyWithdrawn) GetType() string {
	return "MoneyWithdrawn"
}

type MoneyDeposited struct {
	Money int64 `json:"money,omitempty"`
}

func (_ MoneyDeposited) GetType() string {
	return "MoneyDeposited"
}

type OwnerUpdated struct {
	Owner string `json:"owner,omitempty"`
}

func (_ OwnerUpdated) GetType() string {
	return "OwnerUpdated"
}

type AggregateFactory struct{}

func (AggregateFactory) New(kind string) (eventstore.Typer, error) {
	var e eventstore.Typer
	switch kind {
	case "Account":
		e = NewAccount()
	}
	if e == nil {
		return nil, faults.Errorf("Unknown aggregate kind: %s", kind)
	}
	return e, nil
}

type EventFactory struct{}

func (EventFactory) New(kind string) (eventstore.Typer, error) {
	var e eventstore.Typer
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

func CreateAccount(owner string, id string, money int64) *Account {
	a := &Account{
		Status:  OPEN,
		Balance: money,
		Owner:   owner,
	}
	a.RootAggregate = eventstore.NewRootAggregate(a)
	a.RootAggregate.ID = id
	a.ApplyChange(AccountCreated{
		ID:    id,
		Money: money,
		Owner: owner,
	})
	return a
}

func NewAccount() *Account {
	a := &Account{}
	a.RootAggregate = eventstore.NewRootAggregate(a)
	return a
}

type Account struct {
	eventstore.RootAggregate
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

func (a *Account) HandleEvent(event eventstore.Eventer) {
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

func ApplyChangeFromHistory(es eventstore.EventStore, agg eventstore.Aggregater, e eventstore.Event) error {
	m := eventstore.EventMetadata{
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
