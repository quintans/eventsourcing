package test

import (
	"fmt"

	"github.com/quintans/eventstore"
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

func (_ AccountCreated) EventName() string {
	return "AccountCreated"
}

type MoneyWithdrawn struct {
	Money int64 `json:"money,omitempty"`
}

func (_ MoneyWithdrawn) EventName() string {
	return "MoneyWithdrawn"
}

type MoneyDeposited struct {
	Money int64 `json:"money,omitempty"`
}

func (_ MoneyDeposited) EventName() string {
	return "MoneyDeposited"
}

type OwnerUpdated struct {
	Owner string `json:"owner,omitempty"`
}

func (_ OwnerUpdated) EventName() string {
	return "OwnerUpdated"
}

type Instantiater struct{}

func (_ Instantiater) Instantiate(event eventstore.Event) (eventstore.Eventer, error) {
	var e eventstore.Eventer
	switch event.Kind {
	case "AccountCreated":
		e = AccountCreated{}
	case "MoneyDeposited":
		e = MoneyDeposited{}
	case "MoneyWithdrawn":
		e = MoneyWithdrawn{}
	case "OwnerUpdated":
		e = OwnerUpdated{}
	}
	if e == nil {
		return nil, fmt.Errorf("Unknown event kind: %s", event.Kind)
	}
	err := event.Decode(&e)
	if err != nil {
		return nil, err
	}

	return e, nil
}

func CreateAccount(owner string, id string, money int64) *account {
	a := &account{
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

func NewAccount() *account {
	a := &account{}
	a.RootAggregate = eventstore.NewRootAggregate(a)
	return a
}

type account struct {
	eventstore.RootAggregate
	Status  Status `json:"status,omitempty"`
	Balance int64  `json:"balance,omitempty"`
	Owner   string `json:"owner,omitempty"`
}

func (a account) GetType() string {
	return "Account"
}

func (a *account) Withdraw(money int64) bool {
	if a.Balance >= money {
		a.ApplyChange(MoneyWithdrawn{Money: money})
		return true
	}
	return false
}

func (a *account) Deposit(money int64) {
	a.ApplyChange(MoneyDeposited{Money: money})
}

func (a *account) UpdateOwner(owner string) {
	a.ApplyChange(OwnerUpdated{Owner: owner})
}

func (a *account) HandleEvent(event eventstore.Eventer) {
	switch v := event.(type) {
	case AccountCreated:
		a.HandleAccountCreated(v)
	case MoneyDeposited:
		a.HandleMoneyDeposited(v)
	case MoneyWithdrawn:
		a.HandleMoneyWithdrawn(v)
	case OwnerUpdated:
		a.HandleOwnerUpdated(v)
	}
}

func (a *account) HandleAccountCreated(event AccountCreated) {
	a.ID = event.ID
	a.Balance = event.Money
	// this reflects that we are handling domain events and NOT property events
	a.Status = OPEN
}

func (a *account) HandleMoneyDeposited(event MoneyDeposited) {
	a.Balance += event.Money
}

func (a *account) HandleMoneyWithdrawn(event MoneyWithdrawn) {
	a.Balance -= event.Money
}

func (a *account) HandleOwnerUpdated(event OwnerUpdated) {
	a.Owner = event.Owner
}
