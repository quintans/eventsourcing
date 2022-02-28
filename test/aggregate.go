package test

import (
	"github.com/oklog/ulid/v2"

	"github.com/quintans/eventsourcing/jsoncodec"

	"github.com/quintans/eventsourcing"
)

var (
	TypeAccount        = eventsourcing.Kind("Account")
	KindAccountCreated = eventsourcing.Kind("AccountCreated")
	KindMoneyDeposited = eventsourcing.Kind("MoneyDeposited")
	KindMoneyWithdrawn = eventsourcing.Kind("MoneyWithdrawn")
	KindOwnerUpdated   = eventsourcing.Kind("OwnerUpdated")
)

type Status string

const (
	OPEN   Status = "OPEN"
	CLOSED Status = "CLOSED"
	FROZEN Status = "FROZEN"
)

type AccountCreated struct {
	ID    ulid.ULID `json:"id,omitempty"`
	Money int64     `json:"money,omitempty"`
	Owner string    `json:"owner,omitempty"`
}

func (e AccountCreated) GetType() eventsourcing.Kind {
	return "AccountCreated"
}

type MoneyWithdrawn struct {
	Money int64 `json:"money,omitempty"`
}

func (e MoneyWithdrawn) GetType() eventsourcing.Kind {
	return "MoneyWithdrawn"
}

type MoneyDeposited struct {
	Money int64 `json:"money,omitempty"`
}

func (e MoneyDeposited) GetType() eventsourcing.Kind {
	return "MoneyDeposited"
}

type OwnerUpdated struct {
	Owner string `json:"owner,omitempty"`
}

func (e OwnerUpdated) GetType() eventsourcing.Kind {
	return "OwnerUpdated"
}

func NewJSONCodec() *jsoncodec.Codec {
	c := jsoncodec.New()
	c.RegisterFactory(TypeAccount, func() eventsourcing.Kinder {
		return NewAccount()
	})
	c.RegisterFactory(KindAccountCreated, func() eventsourcing.Kinder {
		return &AccountCreated{}
	})
	c.RegisterFactory(KindMoneyDeposited, func() eventsourcing.Kinder {
		return &MoneyDeposited{}
	})
	c.RegisterFactory(KindMoneyWithdrawn, func() eventsourcing.Kinder {
		return &MoneyWithdrawn{}
	})
	c.RegisterFactory(KindOwnerUpdated, func() eventsourcing.Kinder {
		return &OwnerUpdated{}
	})
	return c
}

func CreateAccount(owner string, id ulid.ULID, money int64) *Account {
	a := NewAccount()
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
	eventsourcing.RootAggregate `json:"-"`

	id      ulid.ULID
	status  Status
	balance int64
	owner   string
}

func (a Account) GetID() string {
	return a.id.String()
}

func (a Account) ID() ulid.ULID {
	return a.id
}

func (a Account) Status() Status {
	return a.status
}

func (a Account) Balance() int64 {
	return a.balance
}

func (a Account) Owner() string {
	return a.owner
}

func (a *Account) Forget() {
	a.HandleEvent(OwnerUpdated{})
}

func (a Account) GetType() eventsourcing.Kind {
	return TypeAccount
}

func (a *Account) Withdraw(money int64) bool {
	if a.balance >= money {
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
	a.id = event.ID
	a.balance = event.Money
	a.owner = event.Owner
	// this reflects that we are handling domain events and NOT property events
	a.status = OPEN
}

func (a *Account) HandleMoneyDeposited(event MoneyDeposited) {
	a.balance += event.Money
}

func (a *Account) HandleMoneyWithdrawn(event MoneyWithdrawn) {
	a.balance -= event.Money
}

func (a *Account) HandleOwnerUpdated(event OwnerUpdated) {
	a.owner = event.Owner
}
