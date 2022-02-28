package test

import (
	"github.com/oklog/ulid/v2"

	"github.com/quintans/eventsourcing/jsoncodec"

	"github.com/quintans/eventsourcing"
)

var (
	TypeAccount        = eventsourcing.AggregateType("Account")
	KindAccountCreated = eventsourcing.EventKind("AccountCreated")
	KindMoneyDeposited = eventsourcing.EventKind("MoneyDeposited")
	KindMoneyWithdrawn = eventsourcing.EventKind("MoneyWithdrawn")
	KindOwnerUpdated   = eventsourcing.EventKind("OwnerUpdated")
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

func NewJSONCodec() *jsoncodec.Codec {
	c := jsoncodec.New()
	c.RegisterFactory(TypeAccount.String(), func() interface{} {
		return NewAccount()
	})
	c.RegisterFactory(KindAccountCreated.String(), func() interface{} {
		return &AccountCreated{}
	})
	c.RegisterFactory(KindMoneyDeposited.String(), func() interface{} {
		return &MoneyDeposited{}
	})
	c.RegisterFactory(KindMoneyWithdrawn.String(), func() interface{} {
		return &MoneyWithdrawn{}
	})
	c.RegisterFactory(KindOwnerUpdated.String(), func() interface{} {
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

func (a Account) GetType() string {
	return TypeAccount.String()
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
