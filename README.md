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
WHERE id >= $1 AND created_at <= NOW()::TIMESTAMP - INTERVAL'1 seconds'
ORDER BY id ASC
LIMIT 100
```

Using this approach, systems like RDBMS like Cockroach can be used.

This is the solution that I will be implementing.

## Core Concepts

The presented solution will use PostgreSQL, and SERIAL to uniquely identify an event.

Snapshots is a technic used to improve the performance of the event store, when retrieving an aggregate, but they don't play any part in keeping the consistency of the event store, therefore if we sporadically fail to save a snapshot, it is not a problem, so they can be saved in a separate go routine.
