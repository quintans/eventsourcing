# Event Store
a simple implementation of an event store using only a database

## Introduction

In most of the implementations that I have seen, they all use some sort of message queue to deliver the domain events to external consumers. But there is a problem with this. It is not possible to write into a database and publish to a message queue in a transaction. Many things can go wrong, like for example, after writing to the database we fail to write into the event message queue. Even if it fails, there is no guarantee that it wasn't published.

Other solutions rely on some kind of sequence number, like serial in Postgres, to track the next record to be read or to be published to a message queue.
This is wrong. Records will not become visible in the same order as the sequence number. 
Consider two concurrent transactions. One acquires the ID 100 and the other the ID 101. If the one with ID 101 is faster to finish the transaction, it will show up first in a query than the one with ID 100.

If the tracker service relies of this number to determine from where to pull, it could miss the last added record, and this could lead to events not being tracked.
Sequence numbers can only guarantee that they are unique, nothing else.

But there is a solution. We can introduce a latency in the que tracking queries, to allow for the concurrent transactions to complete. The track system will then only query up to the current time minus a safety margin.

```sql
SELECT *
FROM events 
WHERE id >= $1
AND created_at <= NOW()::TIMESTAMP - INTERVAL'1 seconds'
ORDER BY id ASC
LIMIT 100
```

Using this approach, systems like RDBMS like Cockroach can be used.

This solution could evolve by plugging in a message queue. This MQ could be feeded the tracker described above.

` events -> Event Store <- tracker process -> Message Queue <- projectors`

## Core Concepts

### ID

The presented solution uses PostgreSQL, but instead of SERIAL to uniquely identify an event, I will be using [xid](https://github.com/rs/xid)

### Snapshots

I will also use the memento pattern, to take snapshots of the current state, every X events.

Snapshots is a technique used to improve the performance of the event store, when retrieving an aggregate, but they don't play any part in keeping the consistency of the event store, therefore if we sporadically fail to save a snapshot, it is not a problem, so they can be saved in a separate go routine.

### Reactor Idempotency

When saving an aggregate, we have the option to supply an idempotent key. Later, we can check the presence of the idempotency key, to see if we are repeating an action. This can be useful when used in process manager reactors.

In all the examples I've seen, about implementing a process manager, it is not clear what the value is in breaking into several subscribers to handle every step of the process, and I think this is because they only consider the happy paths.
If the process is only considering the happy path, there is no advantage in having several subscribers, by the contrary.
But, if we introduce compensation actions, it becomes clear that there is an advantage in using a several subscribers (this is a state machine)

In the following example we exemplify a money transfer with rollback actions, levering idempotent keys.

> I don't see the need to use command handlers in the following examples

```go
func NewTransferReactor(es EventStore) {
    // ...
	l := NewListener(es)
	cancel, err := l.Listen(ctx, func(c context.Context, e Event) {
        switch e.Kind {
        case "TransferStarted":
            OnTransferStarted(c, es, e)
        case "MoneyWithdrawn":
            OnMoneyWithdrawn(c, es, e)
        case "MoneyDeposited":
            OnMoneyDeposited(c, es, e)
        case "TransferFailedToDeposit":
            OnTransferFailedToDeposit(c, es, e)
        }
    })
    // ...
}

func OnTransferStarted(ctx context.Context, es EventStore, e Event) {
    event = NewTransferStarted(e)
    
    // event.Transaction is the idempotent key for the account withdrawal
    exists, _ := es.HasIdempotencyKey(ctx, event.FromAccount, event.Transaction)
    if !exists {
        account := NewAccount()
        es.GetByID(ctx, event.FromAccount, &account)
        if ok := account.Withdraw(event.Amount, event.Transaction); !ok {
            idempotentKey := event.Transaction + "/no-withdraw"
            exists, _ = es.HasIdempotencyKey(ctx, event.FromAccount, idempotentKey)
            if !exists {
                transfer := NewTransfer()
                es.GetByID(ctx, transfer.GetID(), &transfer)
                transfer.NotEnoughFunds()
                es.Save(ctx, transfer, Options{
                    IdempotencyKey: idempotentKey,
                })
            }
            return
        }
        es.Save(ctx, account, Options{
            IdempotencyKey: event.Transaction,
        })
    }
}

func OnMoneyWithdrawn(ctx context.Context, es EventStore, e Event) {
    event := NewMoneyWithdrawnEvent(e)
    
    idempotentKey := event.Transaction + "/debit"
    exists, _ = es.HasIdempotencyKey(ctx, transfer.ToAccount, idempotentKey)
    if !exists {
        transfer = NewTransfer()
        es.GetByID(ctx, transfer.GetID(), &transfer)

        transfer.Debited()
        es.Save(ctx, transfer, Options{
            IdempotencyKey: idempotentKey,
        })
    }

    exists, _ = es.HasIdempotencyKey(ctx, transfer.ToAccount, transfer.Transaction)
    if !exists {
        account := NewAccount()
        es.GetByID(ctx, transfer.ToAccount, &account)
        if ok := account.Deposit(transfer.Amount, transfer.Transaction); !pk {
            idempotentKey := event.Transaction + "/no-deposit"
            exists, _ = es.HasIdempotencyKey(ctx, event.FromAccount, idempotentKey)
            if !exists {
                transfer := NewTransfer()
                es.GetByID(ctx, transfer.GetID(), &transfer)
                transfer.FailedToDeposit()
                es.Save(ctx, transfer, Options{
                    IdempotencyKey: idempotentKey,
                })
            }
            return
        }
        es.Save(ctx, account, Options{
            IdempotencyKey: transfer.Transaction,
        })
    }
}

func OnMoneyDeposited(ctx context.Context, es EventStore, e Event) {
    event := NewMoneyDepositedEvent(e)
    
    idempotentKey := event.Transaction + "/credited"
    exists, _ = es.HasIdempotencyKey(ctx, event.Transaction, idempotentKey)
    if !exists {
        transfer = NewTransfer()
        es.GetByID(ctx, transfer.GetID(), &transfer)

        transfer.Credited()
        es.Save(ctx, transfer, Options{
            IdempotencyKey: idempotentKey,
        })
    }

    completeTransfer(es, event.Transaction)
}

func OnTransferFailedToDeposit(ctx context.Context, es EventStore, e Event) {
    event := NewTransferFailedToDepositEvent(e)

    idempotentKey := event.FromAccount + "/refund"
    exists, _ = es.HasIdempotencyKey(ctx, event.FromAccount, idempotentKey)
    if !exists {
        account := NewAccount()
        es.GetByID(ctx, event.FromAccount, &account)
        account.Refund(event.Amount, event.Transaction)
        es.Save(ctx, account, Options{
            IdempotencyKey: idempotentKey,
        })
    }

    completeTransfer(es, event.Transaction)
}

func completeTransfer(es EventStore, txID string) {
    idempotentKey := txID + "/complete"
    exists, _ = es.HasIdempotencyKey(ctx, txID, idempotentKey)
    if !exists {
        transfer = NewTransfer()
        es.GetByID(ctx, transfer.GetID(), &transfer)

        transfer.Completed()
        es.Save(ctx, transfer, Options{
            IdempotencyKey: idempotentKey,
        })
    }
}
```
