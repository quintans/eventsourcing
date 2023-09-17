package test

import (
	"errors"
	"strings"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding/jsoncodec"
	"github.com/quintans/eventsourcing/util/ids"
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

func NewJSONCodecWithUpcaster() *jsoncodec.Codec[ids.AggID] {
	c := jsoncodec.New[ids.AggID]()
	c.RegisterFactory(KindAccount, func(id ids.AggID) eventsourcing.Kinder {
		return DehydratedAccount(id)
	})
	c.RegisterUpcaster(KindAccount, func(t eventsourcing.Kinder) (eventsourcing.Kinder, error) {
		acc := t.(*Account)
		acc2 := DehydratedAccountV2(acc.GetID())
		acc2.status = acc.status
		acc2.balance = acc.balance
		owner, err := toNameVO(acc.owner)
		if err != nil {
			return nil, faults.Errorf("failed to convert owner '%s': %w", acc.owner, err)
		}
		acc2.owner = owner
		return acc2, nil
	})
	c.RegisterFactory(KindAccountV2, func(id ids.AggID) eventsourcing.Kinder {
		return DehydratedAccountV2(id)
	})

	c.RegisterFactory(KindAccountCreated, func(_ ids.AggID) eventsourcing.Kinder {
		return &AccountCreated{}
	})
	c.RegisterUpcaster(KindAccountCreated, func(t eventsourcing.Kinder) (eventsourcing.Kinder, error) {
		created := t.(*AccountCreated)
		return migrateAccountCreated(*created)
	})
	c.RegisterFactory(KindAccountCreatedV2, func(_ ids.AggID) eventsourcing.Kinder {
		return &AccountCreatedV2{}
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
	c.RegisterUpcaster(KindOwnerUpdated, func(t eventsourcing.Kinder) (eventsourcing.Kinder, error) {
		created := t.(*OwnerUpdated)
		return migrateOwnerUpdated(*created)
	})
	c.RegisterFactory(KindOwnerUpdatedV2, func(_ ids.AggID) eventsourcing.Kinder {
		return &OwnerUpdatedV2{}
	})
	return c
}

func NewAccountV2(owner NameVO, money int64) (*AccountV2, error) {
	a := DehydratedAccountV2(ids.New())
	if err := a._root.ApplyChange(&AccountCreatedV2{
		Money: money,
		Owner: owner,
	}); err != nil {
		return nil, err
	}
	return a, nil
}

func DehydratedAccountV2(id ids.AggID) *AccountV2 {
	a := &AccountV2{}
	a._root = eventsourcing.NewRootAggregate(a, id)
	return a
}

type AccountV2 struct {
	_root eventsourcing.RootAggregate[ids.AggID]

	status  Status
	balance int64
	owner   NameVO
}

func (a *AccountV2) PopEvents() []eventsourcing.Eventer {
	return a._root.PopEvents()
}

func (a *AccountV2) GetID() ids.AggID {
	return a._root.GetID()
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
		if err := a._root.ApplyChange(&MoneyWithdrawn{Money: money}); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (a *AccountV2) Deposit(money int64) error {
	return a._root.ApplyChange(&MoneyDeposited{Money: money})
}

func (a *AccountV2) UpdateOwner(owner string) error {
	return a._root.ApplyChange(&OwnerUpdated{Owner: owner})
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

func MigrateAccountCreated[K eventsourcing.ID](e *eventsourcing.Event[K], codec eventsourcing.Codec[K]) (*eventsourcing.EventMigration, error) {
	// will upcast
	event, err := codec.Decode(e.Body, eventsourcing.DecoderMeta[K]{
		Kind: e.Kind,
	})
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

func MigrateOwnerUpdated[K eventsourcing.ID](e *eventsourcing.Event[K], codec eventsourcing.Codec[K]) (*eventsourcing.EventMigration, error) {
	// will upcast
	event, err := codec.Decode(e.Body, eventsourcing.DecoderMeta[K]{
		Kind: e.Kind,
	})
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
