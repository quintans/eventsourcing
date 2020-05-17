# Event Store
a simple implementation of an event store using only a database

## Introduction

In most of the implementations that I have seen, they all use some sort of bus to deliver the domain events to external consumers. But there is a problem with this. It is not possible to write into a database and publish to a bus in a transaction. Many things can go wrong, like for example, after writing to the database we fail to write into the event bus. Even if it fails, there is no guarantee that it wasn't published.

Using a RDBMS we could eliminate the inconsistency, between the database and the bus, if we use a process manager (aka "saga").
Inside a database transaction, we publish to a validation topic. If the publish fails, we rollback. If the transaction fails, after the publish, we can still check if the event is there, and only then publish to the final topic.

open tx -> db write -> pub to validation -> close tx -> sub validate -> in db -> pub to final topic  

The above solution does not fit to NoSQL databases.

I think that a simpler solution is to ignore the use of an event bus and replace with a poll strategy directly to the database.
The throughput that we get is more than enough for most of the cases.

## Core Concepts

The presented solution makes use of [K-Sortable Globally Unique IDs](https://github.com/segmentio/ksuid) and version to uniquely identify an event.

What is important is to guarantee that for an aggregate, the versions are in the right order and without gaps.
The strict order of events, for different aggregates is unimportant. With ksuid events are ordered close to each other in time.
So events are ordered by aggregate ID and Version.

Snapshots is a technic used to improve the performance of the event store, when retrieving an aggregate, but they don't play any part in keeping the consistency of the event store, therefore if we sporadically fail to save a snapshot, it is not a problem.

This approach to primary keys and snapshots allows us to apply this solution to RDBMS and NoSQL databases.
