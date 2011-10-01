
# Design doc: queue replication

## Goals

- data duplication for disaster recovery

## Non-goals

- speed
- automated replica distribution
- 

## Motivation

Some slow-moving queues may contain valuable data. The loss of this data would
be tragic enough that it's worth paying a performance penalty for duplicating
the data across the network, to ensure that if one entire machine dies, no
data that's been "confirmed written" is lost.

## High-level overview

The "McBride method":

A replicated queue Q on server A (the "primary") will have a connection to
server B (the "replica"), and send journal updates as they happen on disk.
Server B will operate this queue just like a normal queue, but hidden from
clients. If server A drops its connection and doesn't reconnect within time
T, server B will dump items from its hidden copy of the queue into its own
queue Q.

## Penalty

Because each replicated queue will be written twice, to two different servers,
they will have the following penalties, per replicated queue:

- 2x disk I/O (since each machine will have its own queue and a copy of
  another server's queue, both of which must be journaled)
- 2x network bandwidth
- if clients wait for the server ack, PUTs will be slower by the amount of a
  network roundtrip

## Implementation

Each replicated queue will have a function configured to tell it which
host/port to connect to for writing the replica. A default function will be
provided, which just takes a server list, and uses the server "after" the
current server. (So each server will replicate to the neighbor to its right.)

A special thrift protocol will be used for replication. The protocol will have
two operations sent by the primary host:

- put(msg#, queue_name, item#, item)
- get(msg#, queue_name, list<item#s>)

Each operation has a monotonically increasing message number. If this number
ever falls out of order, the connection is corrupted and the replica host
must disconnect.

The replica will periodically send back:

- ack(msg#)

which acknowledges all messages up to and including that one.

## Edge cases

Replication is all about the handling of edge cases.

### Open (unconfirmed) reads

No "open reads" (GETs awaiting confirmation) will be shared across
replication. A confirmed read will be treated as a normal GET. As usual, the
tradeoff is to allow for duplicated data instead of lost data.

### Fanout queues

Only the parent queue can be replicated. A "get" is replicated when the last
child has consumed the item.

When a replicating server dies, the replication target adds all the items (the
ones that hadn't been read by all children) to its own queue, possibly causing
duplication of data: items not read by the dead server's slowest child queue
will appear again in all of the live server's child queues.

### Disconnects

If the connection between the primary host and the replica host is lost, it's
the primary host's responsibility to reconnect. If no connection is made
within a configured timeout, the replica will assume the primary is dead,
flush any replicated items into its own queue, and forget all replication
state.

On reconnect, the primary sends the last message number it sent, or 0 if it
has no previous state. The replica sends the last message number it's seen,
or 0 if it has no previous state. If either side sends 0, replication starts
from scratch: the primary dumps the current queue state. Otherwise, the
primary retransmits any messages the replica didn't get, and continues.

While disconnected, the primary should buffer replication messages to send
when the connection is re-established. If no connection is established after
a configurable timeout, the primary should forget its replication state, so
it doesn't buffer forever.

### Replica permanently down

If a server crashes and never comes back, the replica list needs to be updated
so that the remaining servers don't try to replicate to it.

In a serverset world, this updating could happen automatically, but will need
to be debounced so that the server is considered "permanently gone" only after
a reasonably long period of time.
