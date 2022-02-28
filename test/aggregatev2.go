package test

import (
	"errors"
	"strings"

	"github.com/oklog/ulid/v2"

	"github.com/quintans/eventsourcing/jsoncodec"

	"github.com/quintans/eventsourcing"
)

// This represent the new software version after a migration
// Using the suffix V2 should only be used by the migrate events. Here we use everywhere to be easy to

var (
	KindAccountCreatedV2 = eventsourcing.EventKind("AccountCreated_V2")
	KindOwnerUpdatedV2   = eventsourcing.EventKind("OwnerUpdated_V2")
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
	ID    ulid.ULID `json:"id,omitempty"`
	Money int64     `json:"money,omitempty"`
	Owner NameVO
}

func (e AccountCreatedV2) GetType() string {
	return "AccountCreated_V2"
}

type OwnerUpdatedV2 struct {
	Owner NameVO `json:"owner,omitempty"`
}

func (e OwnerUpdatedV2) GetType() string {
	return "OwnerUpdated_V2"
}

func NewJSONCodecV2() *jsoncodec.Codec {
	c := jsoncodec.New()
	c.RegisterFactory(TypeAccount.String(), func() interface{} {
		return NewAccount()
	})
	c.RegisterFactory(TypeAccount.String(), func() interface{} {
		return NewAccountV2()
	})
	c.RegisterFactory(KindAccountCreated.String(), func() interface{} {
		return &AccountCreated{}
	})
	c.RegisterFactory(KindAccountCreatedV2.String(), func() interface{} {
		return &AccountCreatedV2{}
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
	c.RegisterFactory(KindOwnerUpdatedV2.String(), func() interface{} {
		return &OwnerUpdatedV2{}
	})
	return c
}

func CreateAccountV2(owner NameVO, id ulid.ULID, money int64) *AccountV2 {
	a := NewAccountV2()
	a.ApplyChange(AccountCreatedV2{
		ID:    id,
		Money: money,
		Owner: owner,
	})
	return a
}

func NewAccountV2() *AccountV2 {
	a := &AccountV2{}
	a.RootAggregate = eventsourcing.NewRootAggregate(a)
	return a
}

type AccountV2 struct {
	eventsourcing.RootAggregate `json:"-"`

	id      ulid.ULID
	status  Status
	balance int64
	owner   NameVO
}

func (a AccountV2) GetID() string {
	return a.id.String()
}

func (a AccountV2) ID() ulid.ULID {
	return a.id
}

func (a AccountV2) Status() Status {
	return a.status
}

func (a AccountV2) Balance() int64 {
	return a.balance
}

func (a AccountV2) Owner() NameVO {
	return a.owner
}

func (a AccountV2) GetType() string {
	return "Account"
}

func (a *AccountV2) Withdraw(money int64) bool {
	if a.balance >= money {
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
	a.id = event.ID
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
	event, err := codec.Decode(e.Body, string(e.AggregateType))
	if err != nil {
		return nil, err
	}
	oldEvent := event.(*AccountCreated)
	first, last := SplitName(oldEvent.Owner)
	owner, err := NewName(first, last)
	if err != nil {
		return nil, err
	}
	newEvent := AccountCreatedV2{
		ID:    oldEvent.ID,
		Money: oldEvent.Money,
		Owner: owner,
	}
	body, err := codec.Encode(newEvent)
	if err != nil {
		return nil, err
	}

	m := eventsourcing.DefaultEventMigration(e)
	m.Kind = KindAccountCreatedV2
	m.Body = body

	return m, nil
}

func MigrateOwnerUpdated(e *eventsourcing.Event, codec eventsourcing.Codec) (*eventsourcing.EventMigration, error) {
	event, err := codec.Decode(e.Body, string(e.AggregateType))
	if err != nil {
		return nil, err
	}
	oldEvent := event.(*OwnerUpdated)
	first, last := SplitName(oldEvent.Owner)
	owner, err := NewName(first, last)
	if err != nil {
		return nil, err
	}
	newEvent := OwnerUpdatedV2{
		Owner: owner,
	}
	body, err := codec.Encode(newEvent)
	if err != nil {
		return nil, err
	}

	m := eventsourcing.DefaultEventMigration(e)
	m.Kind = KindOwnerUpdatedV2
	m.Body = body

	return m, nil
}

func SplitName(name string) (string, string) {
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
	return first, last
}
