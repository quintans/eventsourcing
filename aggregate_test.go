package eventstore

import "encoding/json"

type RootAggregate struct {
	ID      string `json:"id,omitempty"`
	Version int    `json:"version,omitempty"`

	events       []interface{}
	eventVersion int
}

func (a RootAggregate) GetID() string {
	return a.ID
}

func (a RootAggregate) GetVersion() int {
	return a.Version
}

func (a *RootAggregate) SetVersion(version int) {
	a.Version = version
}

func (a RootAggregate) GetEvents() []interface{} {
	return a.events
}

func (a *RootAggregate) ClearEvents() {
	a.events = []interface{}{}
}

type Status string

const (
	OPEN   Status = "OPEN"
	CLOSED Status = "CLOSED"
	FROZEN Status = "FROZEN"
)

type AccountCreated struct {
	ID    string `json:"id,omitempty"`
	Money int64  `json:"money,omitempty"`
}

type MoneyWithdrawn struct {
	Money int64 `json:"money,omitempty"`
}

type MoneyDeposited struct {
	Money int64 `json:"money,omitempty"`
}

func CreateAccount(id string, money int64) *Account {
	a := &Account{
		RootAggregate: RootAggregate{
			ID: id,
		},
		Status:  OPEN,
		Balance: money,
	}
	a.events = []interface{}{
		AccountCreated{
			ID:    id,
			Money: money,
		},
	}
	return a
}

func NewAccount() *Account {
	a := &Account{
		RootAggregate: RootAggregate{
			events: []interface{}{},
		},
	}
	return a
}

type Account struct {
	RootAggregate
	Status  Status `json:"status,omitempty"`
	Balance int64  `json:"balance,omitempty"`
}

func (a *Account) ApplyChangeFromHistory(event Event) error {
	var evt interface{}
	switch event.Kind {
	case "AccountCreated":
		evt = AccountCreated{}
	case "MoneyDeposited":
		evt = MoneyDeposited{}
	case "MoneyWithdrawn":
		evt = MoneyWithdrawn{}
	}
	if err := json.Unmarshal(event.Body, &evt); err != nil {
		return err
	}

	a.applyChange(evt, false)
	a.Version = event.AggregateVersion
	a.eventVersion = a.Version
	return nil
}

func (a *Account) applyChange(event interface{}, isNew bool) {
	switch t := event.(type) {
	case AccountCreated:
		a.HandleAccountCreated(t)
	case MoneyDeposited:
		a.HandleMoneyDeposited(t)
	case MoneyWithdrawn:
		a.HandleMoneyWithdrawn(t)
	}

	if isNew {
		a.events = append(a.events, event)
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

func (a *Account) Withdraw(money int64) bool {
	if a.Balance >= money {
		a.applyChange(MoneyWithdrawn{Money: money}, true)
		return true
	}
	return false
}

func (a *Account) Deposit(money int64) {
	a.applyChange(MoneyDeposited{Money: money}, true)
}
