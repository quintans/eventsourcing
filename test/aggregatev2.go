package test

import (
	"errors"
	"strings"

	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding/jsoncodec"
)

// This represent the new software version after a migration
// Using the suffix V2 should only be used by the migrate events. Here we use everywhere to be easy to

var (
	KindAccountV2        = eventsourcing.Kind("Account_V2")
	KindAccountCreatedV2 = eventsourcing.Kind("AccountCreated_V2")
	KindOwnerUpdatedV2   = eventsourcing.Kind("OwnerUpdated_V2")
)

type NameVO struct {
	firstName string
	lastName  string
}

func NewName(firstName string, lastName string) (NameVO, error) {
	if firstName == "" {
		return NameVO{}, errors.New("first name cannot be empty")
	}
	if lastName == "" {
		return NameVO{}, errors.New("last name cannot be empty")
	}
	return NameVO{
		firstName: firstName,
		lastName:  lastName,
	}, nil
}

func (n NameVO) FirstName() string {
	return n.firstName
}

func (n NameVO) LastName() string {
	return n.lastName
}

type AccountCreatedV2 struct {
	Id    ulid.ULID
	Money int64
	Owner NameVO
}

func (e AccountCreatedV2) GetKind() eventsourcing.Kind {
	return KindAccountCreatedV2
}

type OwnerUpdatedV2 struct {
	Owner NameVO
}

func (e OwnerUpdatedV2) GetKind() eventsourcing.Kind {
	return KindOwnerUpdatedV2
}

func NewJSONCodecWithUpcaster() *jsoncodec.Codec {
	c := jsoncodec.New()
	c.RegisterFactory(KindAccount, func() eventsourcing.Kinder {
		return NewAccount()
	})
	c.RegisterUpcaster(KindAccount, func(t eventsourcing.Kinder) (eventsourcing.Kinder, error) {
		acc := t.(*Account)
		acc2 := NewAccountV2()
		acc2.id = acc.id
		acc2.status = acc.status
		acc2.balance = acc.balance
		owner, err := toNameVO(acc.owner)
		if err != nil {
			return nil, faults.Errorf("failed to convert owner '%s': %w", acc.owner, err)
		}
		acc2.owner = owner
		return acc2, nil
	})
	c.RegisterFactory(KindAccountV2, func() eventsourcing.Kinder {
		return NewAccountV2()
	})

	c.RegisterFactory(KindAccountCreated, func() eventsourcing.Kinder {
		return &AccountCreated{}
	})
	c.RegisterUpcaster(KindAccountCreated, func(t eventsourcing.Kinder) (eventsourcing.Kinder, error) {
		created := t.(*AccountCreated)
		return migrateAccountCreated(*created)
	})
	c.RegisterFactory(KindAccountCreatedV2, func() eventsourcing.Kinder {
		return &AccountCreatedV2{}
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
	c.RegisterUpcaster(KindOwnerUpdated, func(t eventsourcing.Kinder) (eventsourcing.Kinder, error) {
		created := t.(*OwnerUpdated)
		return migrateOwnerUpdated(*created)
	})
	c.RegisterFactory(KindOwnerUpdatedV2, func() eventsourcing.Kinder {
		return &OwnerUpdatedV2{}
	})
	return c
}

func CreateAccountV2(owner NameVO, id ulid.ULID, money int64) (*AccountV2, error) {
	a := NewAccountV2()
	if err := a.root.ApplyChange(&AccountCreatedV2{
		Id:    id,
		Money: money,
		Owner: owner,
	}); err != nil {
		return nil, err
	}
	return a, nil
}

func NewAccountV2() *AccountV2 {
	a := &AccountV2{}
	a.root = eventsourcing.NewRootAggregate(a)
	return a
}

type AccountV2 struct {
	root eventsourcing.RootAggregate `json:"-"`

	id      ulid.ULID
	status  Status
	balance int64
	owner   NameVO
}

func (a *AccountV2) PopEvents() []eventsourcing.Eventer {
	return a.root.PopEvents()
}

func (a *AccountV2) GetID() string {
	return a.id.String()
}

func (a *AccountV2) ID() ulid.ULID {
	return a.id
}

func (a *AccountV2) Status() Status {
	return a.status
}

func (a *AccountV2) Balance() int64 {
	return a.balance
}

func (a *AccountV2) Owner() NameVO {
	return a.owner
}

func (a *AccountV2) GetKind() eventsourcing.Kind {
	return KindAccountV2
}

func (a *AccountV2) Withdraw(money int64) (bool, error) {
	if a.balance >= money {
		if err := a.root.ApplyChange(&MoneyWithdrawn{Money: money}); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (a *AccountV2) Deposit(money int64) error {
	return a.root.ApplyChange(&MoneyDeposited{Money: money})
}

func (a *AccountV2) UpdateOwner(owner string) error {
	return a.root.ApplyChange(&OwnerUpdated{Owner: owner})
}

func (a *AccountV2) HandleEvent(event eventsourcing.Eventer) error {
	switch t := event.(type) {
	case *AccountCreatedV2:
		a.HandleAccountCreatedV2(*t)
	case *MoneyDeposited:
		a.HandleMoneyDeposited(*t)
	case *MoneyWithdrawn:
		a.HandleMoneyWithdrawn(*t)
	case *OwnerUpdatedV2:
		a.HandleOwnerUpdatedV2(*t)
	default:
		return faults.Errorf("unknown event '%s' for '%s'", event.GetKind(), a.GetKind())
	}
	return nil
}

func (a *AccountV2) HandleAccountCreatedV2(event AccountCreatedV2) {
	a.id = event.Id
	a.balance = event.Money
	a.owner = event.Owner
	// this reflects that we are handling domain events and NOT property events
	a.status = OPEN
}

func (a *AccountV2) HandleMoneyDeposited(event MoneyDeposited) {
	a.balance += event.Money
}

func (a *AccountV2) HandleMoneyWithdrawn(event MoneyWithdrawn) {
	a.balance -= event.Money
}

func (a *AccountV2) HandleOwnerUpdatedV2(event OwnerUpdatedV2) {
	a.owner = event.Owner
}

func MigrateAccountCreated(e *eventsourcing.Event, codec eventsourcing.Codec) (*eventsourcing.EventMigration, error) {
	// will upcast
	event, err := codec.Decode(e.Body, e.Kind)
	if err != nil {
		return nil, faults.Errorf("failed do decode '%s': %w", e.AggregateKind, err)
	}
	body, err := codec.Encode(event)
	if err != nil {
		return nil, err
	}

	m := eventsourcing.DefaultEventMigration(e)
	m.Kind = event.GetKind()
	m.Body = body

	return m, nil
}

func MigrateOwnerUpdated(e *eventsourcing.Event, codec eventsourcing.Codec) (*eventsourcing.EventMigration, error) {
	// will upcast
	event, err := codec.Decode(e.Body, e.Kind)
	if err != nil {
		return nil, err
	}
	body, err := codec.Encode(event)
	if err != nil {
		return nil, err
	}

	m := eventsourcing.DefaultEventMigration(e)
	m.Kind = event.GetKind()
	m.Body = body

	return m, nil
}

func migrateAccountCreated(oldEvent AccountCreated) (AccountCreatedV2, error) {
	owner, err := toNameVO(oldEvent.Owner)
	if err != nil {
		return AccountCreatedV2{}, err
	}
	return AccountCreatedV2{
		Id:    oldEvent.Id,
		Money: oldEvent.Money,
		Owner: owner,
	}, nil
}

func migrateOwnerUpdated(oldEvent OwnerUpdated) (OwnerUpdatedV2, error) {
	owner, err := toNameVO(oldEvent.Owner)
	if err != nil {
		return OwnerUpdatedV2{}, err
	}
	return OwnerUpdatedV2{
		Owner: owner,
	}, nil
}

func toNameVO(name string) (NameVO, error) {
	name = strings.TrimSpace(name)
	names := strings.Split(name, " ")
	half := len(names) / 2
	var first, last string
	if half > 0 {
		first = strings.Join(names[:half], " ")
		last = strings.Join(names[half:], " ")
	} else {
		first = names[0]
	}
	return NewName(first, last)
}
