# Event Store
a simple implementation of an event store using only a database

## Introduction

In most of the implementations that I have seen, they all use some sort of message queue to deliver the domain events to external consumers. But there is a problem with this. It is not possible to write into a database and publish to a message queue in a transaction. Many things can go wrong, like for example, after writing to the database we fail to write into the event message queue. Even if it fails, there is no guarantee that it wasn't published.

Other solutions propose a second service that would track the changes in the database using a sequence number and then it would then publish them into a message queue.
But I don't know any database were this number is guaranteed to be greater than any of the existing numbers in database.
A concurrent operation with a smaller number could be slower writing into the database, creating a record with the sequence number out of order.
If the tracker service relies of this number to determine from where to pull, it could miss the last added record, and this could lead to events not being tracked.
Sequence numbers can only guarantee that they are unique, nothing else.

Using a RDBMS we could eliminate the inconsistency, between the database and the message queue, if we use a process manager (aka "saga").
Inside a database transaction, we publish to a validation topic. If the publish fails, we rollback. If the transaction fails, after the publish, we can still check if the event is there, and only then publish to the final outbound topic.

`open tx ( db write -> pub to validation ) -> validate saga -> in db -> pub to outbound topic`

> duplicate events can happen, but it is expected that the consumers are idempotent

This is the solution that I will be implementing.

## Core Concepts

The presented solution makes use of [K-Sortable Globally Unique IDs](https://github.com/segmentio/ksuid) to uniquely identify an event.

Snapshots is a technic used to improve the performance of the event store, when retrieving an aggregate, but they don't play any part in keeping the consistency of the event store, therefore if we sporadically fail to save a snapshot, it is not a problem, so they can be saved in a separate go routine.
