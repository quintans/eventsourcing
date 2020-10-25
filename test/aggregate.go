package test

import (
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

type MoneyWithdrawn struct {
	Money int64 `json:"money,omitempty"`
}

type MoneyDeposited struct {
	Money int64 `json:"money,omitempty"`
}

type OwnerUpdated struct {
	Owner string `json:"owner,omitempty"`
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
