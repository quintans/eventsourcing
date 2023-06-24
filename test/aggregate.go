package test

import (
	"github.com/oklog/ulid/v2"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/jsoncodec"

	"github.com/quintans/eventsourcing"
)

var (
	KindAccount        = eventsourcing.Kind("Account")
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
	Id    ulid.ULID
	Money int64
	Owner string
}

func (e AccountCreated) GetKind() eventsourcing.Kind {
	return KindAccountCreated
}

type MoneyWithdrawn struct {
	Money int64
}

func (e MoneyWithdrawn) GetKind() eventsourcing.Kind {
	return KindMoneyWithdrawn
}

type MoneyDeposited struct {
	Money int64
}

func (e MoneyDeposited) GetKind() eventsourcing.Kind {
	return KindMoneyDeposited
}

type OwnerUpdated struct {
	Owner string
}

func (e OwnerUpdated) GetKind() eventsourcing.Kind {
	return KindOwnerUpdated
}

func NewJSONCodec() *jsoncodec.Codec {
	c := jsoncodec.New()
	c.RegisterFactory(KindAccount, func() eventsourcing.Kinder {
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

func CreateAccount(owner string, id ulid.ULID, money int64) (*Account, error) {
	a := NewAccount()
	if err := a.root.ApplyChange(&AccountCreated{
		Id:    id,
		Money: money,
		Owner: owner,
	}); err != nil {
		return nil, err
	}
	return a, nil
}

func NewAccount() *Account {
	a := &Account{}
	a.root = eventsourcing.NewRootAggregate(a)
	return a
}

type Account struct {
	root eventsourcing.RootAggregate `json:"-"`

	id      ulid.ULID
	status  Status
	balance int64
	owner   string
}

func (a *Account) PopEvents() []eventsourcing.Eventer {
	return a.root.PopEvents()
}

func (a *Account) GetID() string {
	if a == nil {
		return ""
	}
	return a.id.String()
}

func (a *Account) ID() ulid.ULID {
	return a.id
}

func (a *Account) Status() Status {
	return a.status
}

func (a *Account) Balance() int64 {
	return a.balance
}

func (a *Account) Owner() string {
	return a.owner
}

func (a *Account) Forget() {
	a.owner = ""
}

func (a *Account) GetKind() eventsourcing.Kind {
	return KindAccount
}

func (a *Account) Withdraw(money int64) (bool, error) {
	if a.balance >= money {
		err := a.root.ApplyChange(&MoneyWithdrawn{Money: money})
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (a *Account) Deposit(money int64) error {
	return a.root.ApplyChange(&MoneyDeposited{Money: money})
}

func (a *Account) UpdateOwner(owner string) error {
	return a.root.ApplyChange(&OwnerUpdated{Owner: owner})
}

func (a *Account) HandleEvent(event eventsourcing.Eventer) error {
	switch t := event.(type) {
	case *AccountCreated:
		a.HandleAccountCreated(*t)
	case *MoneyDeposited:
		a.HandleMoneyDeposited(*t)
	case *MoneyWithdrawn:
		a.HandleMoneyWithdrawn(*t)
	case *OwnerUpdated:
		a.HandleOwnerUpdated(*t)
	default:
		return faults.Errorf("unknown event '%s' for '%s'", event.GetKind(), a.GetKind())
	}
	return nil
}

func (a *Account) HandleAccountCreated(event AccountCreated) {
	a.id = event.Id
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
