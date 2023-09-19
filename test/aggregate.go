package test

import (
	"github.com/quintans/eventsourcing/encoding/jsoncodec"
	"github.com/quintans/eventsourcing/util/ids"

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
	Money int64
	Owner string
}

func (e *AccountCreated) GetKind() eventsourcing.Kind {
	return KindAccountCreated
}

type MoneyWithdrawn struct {
	Money int64
}

func (e *MoneyWithdrawn) GetKind() eventsourcing.Kind {
	return KindMoneyWithdrawn
}

type MoneyDeposited struct {
	Money int64
}

func (e *MoneyDeposited) GetKind() eventsourcing.Kind {
	return KindMoneyDeposited
}

type OwnerUpdated struct {
	Owner string
}

func (e *OwnerUpdated) GetKind() eventsourcing.Kind {
	return KindOwnerUpdated
}

func NewJSONCodec() *jsoncodec.Codec[ids.AggID] {
	c := jsoncodec.New[ids.AggID]()
	c.RegisterFactory(KindAccount, func(id ids.AggID) eventsourcing.Kinder {
		return DehydratedAccount(id)
	})
	c.RegisterFactory(KindAccountCreated, func(_ ids.AggID) eventsourcing.Kinder {
		return &AccountCreated{}
	})
	c.RegisterFactory(KindMoneyDeposited, func(_ ids.AggID) eventsourcing.Kinder {
		return &MoneyDeposited{}
	})
	c.RegisterFactory(KindMoneyWithdrawn, func(_ ids.AggID) eventsourcing.Kinder {
		return &MoneyWithdrawn{}
	})
	c.RegisterFactory(KindOwnerUpdated, func(_ ids.AggID) eventsourcing.Kinder {
		return &OwnerUpdated{}
	})
	return c
}

func NewAccount(owner string, money int64) (*Account, error) {
	a := DehydratedAccount(ids.New())
	if err := a._reg.ApplyChange(&AccountCreated{
		Money: money,
		Owner: owner,
	}); err != nil {
		return nil, err
	}
	return a, nil
}

func DehydratedAccount(id ids.AggID) *Account {
	reg := eventsourcing.NewRegistry()
	a := &Account{
		RootAggregate: eventsourcing.NewRootAggregate(reg, id),
		_reg:          reg,
	}
	eventsourcing.EventHandler(reg, a.handleAccountCreated)
	eventsourcing.EventHandler(reg, a.handleMoneyDeposited)
	eventsourcing.EventHandler(reg, a.handleMoneyWithdrawn)
	eventsourcing.EventHandler(reg, a.handleOwnerUpdated)
	return a
}

type Account struct {
	eventsourcing.RootAggregate[ids.AggID]
	_reg *eventsourcing.Registry

	status  Status
	balance int64
	owner   string
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
		err := a._reg.ApplyChange(&MoneyWithdrawn{Money: money})
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (a *Account) Deposit(money int64) error {
	return a._reg.ApplyChange(&MoneyDeposited{Money: money})
}

func (a *Account) UpdateOwner(owner string) error {
	return a._reg.ApplyChange(&OwnerUpdated{Owner: owner})
}

func (a *Account) handleAccountCreated(event *AccountCreated) {
	a.balance = event.Money
	a.owner = event.Owner
	// this reflects that we are handling domain events and NOT property events
	a.status = OPEN
}

func (a *Account) handleMoneyDeposited(event *MoneyDeposited) {
	a.balance += event.Money
}

func (a *Account) handleMoneyWithdrawn(event *MoneyWithdrawn) {
	a.balance -= event.Money
}

func (a *Account) handleOwnerUpdated(event *OwnerUpdated) {
	a.owner = event.Owner
}
