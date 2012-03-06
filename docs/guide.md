
A working guide to kestrel
==========================

Kestrel is a very simple message queue that runs on the JVM and uses the
memcache protocol (with some extensions) to talk to clients.

A single kestrel server has a set of queues identified by a name, which is
also the filename of that queue's journal file (usually in
`/var/spool/kestrel`). Each queue is a strictly-ordered FIFO of "items" of
binary data. Usually this data is in some serialized format like JSON or
ruby's marshal format.

Generally queue names should be limited to alphanumerics `[A-Za-z0-9]`, dash
(`-`) and underline (`_`). In practice, kestrel doesn't enforce any
restrictions other than the name can't contain slash (`/`) because that can't
be used in filenames, squiggle (`~`) because it's used for temporary files,
plus (`+`) because it's used for fanout queues, and dot (`.`) because it's
used to distinguish multiple files for the same queue. Queue names are case-
sensitive, but if you're running kestrel on OS X or Windows, you will want to
refrain from taking advantage of this, since the journal filenames on those
two platforms are *not* case-sensitive.

A cluster of kestrel servers is like a memcache cluster: the servers don't
know about each other, and don't do any cross-communication, so you can add as
many as you like. Clients have a list of all servers in the cluster, and pick
one at random for each operation. In this way, each queue appears to be spread
out across every server, with items in a loose ordering.

When kestrel starts up, it scans the journal folder and creates queues based
on any journal files it finds there, to restore state to the way it was when
it last shutdown (or was killed or died). New queues are created by referring
to them (for example, adding or trying to remove an item). A queue can be
deleted with the "delete" command.


Configuration
-------------

The config files for kestrel are scala expressions loaded at runtime, usually
from `production.scala`, although you can use `development.scala` by passing
`-Dstage=development` to the java command line.

The config file evaluates to a `KestrelConfig` object that's used to configure
the server as a whole, a default queue, and any overrides for specific named
queues. The fields on `KestrelConfig` are documented here with their default
values:
[KestrelConfig.html](http://robey.github.com/kestrel/doc/main/api/net/lag/kestrel/config/KestrelConfig.html)

When the server starts up, it logs its configuration, and the configuration of
each queue it finds:

    INF [20120111-16:43:21.878] kestrel: Kestrel config: listenAddress=0.0.0.0
      memcachePort=Some(22133) textPort=Some(2222)
      queuePath=/Users/robey/queues expirationTimerFrequency=Some(1.seconds)
      clientTimeout=Some(30.seconds) maxOpenTransactions=100
    INF [20120111-16:43:22.053] kestrel: Setting up queue spam: name=spam
      maxItemSize=9223372036854775807.bytes journaled=true
      journalSize=16777216.bytes syncJournal=never saveArchivedJournals=None
      checkpointTimer=1.seconds
    INF [20120111-18:32:34.021] kestrel: Queue spam reader <default>:
      maxItems=2147483647 maxSize=9223372036854775807.bytes
      maxMemorySize=134217728.bytes maxAge=None fullPolicy=RefusePuts
      maxExpireSweep=2147483647

In previous versions, you could change the configuration and ask the server to
reload. This was difficult to test, hard to keep working, and apparently not
widely used, so the feature was removed. To use a new configuration, just
restart the server.

Logging is configured according to `util-logging`. The logging configuration
syntax is described here:
[util-logging](https://github.com/twitter/util/blob/master/util-logging/README.markdown)

Per-queue configuration is documented here:
[QueueBuilder.html](http://robey.github.com/kestrel/doc/main/api/net/lag/kestrel/config/QueueBuilder.html)
and here:
[QueueReaderBuilder.html](http://robey.github.com/kestrel/doc/main/api/net/lag/kestrel/config/QueueReaderBuilder.html)

Starting in kestrel 3.0, fanout queues (see the "Fanout Queues" section below)
can be configured independently. A side effect of this is that the reader &
writer side of a queue are now configured separately too. The writer
configuration relates mostly to the journal (which all readers share), and is
in `QueueBuilder`. The reader configuration relates to the in-memory
representation of a queue, and is in `QueueReaderBuilder`.


Full queues
-----------

A queue reader can have the following limits set on it:

- `maxItems` - total items in the queue
- `maxSize` - total bytes of data in the items in the queue

If either of these limits is reached, no new items can be added to the queue.
(Clients will receive an error when trying to add.) If you set `fullPolicy` to
`DropOldest`, then all puts will succeed, and the oldest item(s) will be
silently discarded until the queue is back within the item and size limits.

`maxItemSize` (on the writer side) limits the size of any individual item. If
an add is attempted with an item larger than this limit, it always fails.


Journal files
-------------

Each queue has at least two on-disk journal files, unless it's been configured
to not journal: a write-only journal, and one read journal for each reader.
(Normal queues have exactly one writer and one reader, but fanout queues may
have multiple readers. See "Fanout Queues" below for more details.)

The write-only journal is just a sequential record of each item added to the
queue. The journal is broken up into multiple files, so that as items are
read, old journal files can be erased to save disk space. Kestrel will move to
a new file whenever the current file reaches `journalSize` (16MB by default).

The reader journal files contain info about where the head of the queue is.
When kestrel starts up, it replays each queue's write-only journal from this
head, in order to build up the in-memory representation of the queue that it
uses for client queries.

You can turn the journal off for a queue (`journaled = false`) and the queue
will exist only in memory. If the server restarts, all enqueued items are
lost. You can also force a queue's journal to be sync'd to disk periodically,
or even after every write operation, at a performance cost, using
`syncJournal`.

If a queue grows past `maxMemorySize` bytes (128MB by default), only the first
128MB is kept in memory. The journal is used to track later items, and as
items are removed, the journal is played forward to keep 128MB in memory. This
is known as "read-behind" mode. When a queue is in read-behind mode, removing
an item will often cause a disk operation to read an item in from disk. This
is the trade-off to avoid filling memory and crashing the JVM.


Item expiration
---------------

When they come from a memcache client, expiration times are handled in the
same way as memcache: if the number is small (less than one million), it's
interpreted as a relative number of seconds from now. Otherwise it's
interpreted as an absolute unix epoch time, in seconds since the beginning of
1 January 1970 GMT.

Expiration times are immediately translated into an absolute time, in
*milliseconds*, and if it's further in the future than the queue's `maxAge`,
the `maxAge` is used instead. An expiration of 0, which is usually the
default, means an item never expires.

Expired items are flushed from a queue whenever a new item is added or
removed. Additionally, if the global config option `expirationTimerFrequency`
is set, a background thread will periodically remove expired items from the
head of each queue. The provided `production.conf` sets this to one second.
If this is turned off, an idle queue won't have any items expired, but you
can still trigger a check by doing a "peek" on it.

Normally, expired items are discarded. If `expireToQueue` is set, then
expired items are moved to the specified queue just as if a client had put
it there. The item is added with no expiration time, but that can be
overridden if the new queue has a default expiration policy.

To prevent stalling the server when it encounters a swarm of items that all
expired at the same time, `maxExpireSweep` limits the number of items that
will be removed by the background thread in a single round. This is primarily
useful as a throttling mechanism when using a queue as a way to delay work.

Queue expiration
----------------

Whole queues can be configured to expire as well. If `maxQueueAge` is set 
`expirationTimerFrequency` is used to check the queue age. If the queue is
empty, and it has been longer than `maxQueueAge` since it was created then
the queue will be deleted.

Fanout Queues
-------------

Each queue is conceptually a "writer" (a journal of added items) and at least
one "reader" (an in-memory representation of a queue). Normally, each queue
has one reader, the "default reader", and every item put into the queue is
given to one consumer. This is a standard FIFO queue.

Fanout queues have multiple readers, and behave as if every item put into the
queue was duplicated for each reader. The readers share one (write-only)
journal, but each has a different in-memory queue and a different head
pointer. In this case, the readers each have their own name, which is the
primary queue name followed by a `+` and the reader name. For example, the
"audit" reader of the "orders" queue would be named "`orders+audit`".

Once a queue has at least one named reader, the "default reader" is destroyed.
This means that once you create the "orders+audit" fanout reader, you can no
longer read from the primary "orders" queue. 

When a fanout queue is first referenced by a client, the journal file (if any)
is created, and it will start receiving new items written to the parent queue.
Existing items are not copied over.

A fanout queue can be deleted to stop it from receiving new items. Deleting
all fanout queues will cause the default reader to be re-created, effectively
allowing you to read from the queue named "orders" again.

All of the configuration in `QueueReaderBuilder` can be specified for each
reader independently (which is new in 3.0), so for example, one fanout reader
could be limited to keeping only the most recent 100 items, while another
could enforce a default expiration time. The `defaultReader` is used for all
readers that aren't configured by name.


Thrift protocol
---------------

The thrift protocol is documented in the thrift file here:
[kestrel.thrift](https://github.com/robey/kestrel/blob/master/src/main/thrift/kestrel.thrift)


Memcache commands
-----------------

- `SET <queue-name> <flags (ignored)> <expiration> <# bytes>`

  Add an item to a queue. It may fail if the queue has a size or item limit
  and it's full.

- `GET <queue-name>[options]`

  Remove an item from a queue. It will return an empty response immediately if
  the queue is empty. The queue name may be followed by options separated
  by `/`:

    - `/t=<milliseconds>`

      Wait up to a given time limit for a new item to arrive. If an item arrives
      on the queue within this timeout, it's returned as normal. Otherwise,
      after that timeout, an empty response is returned.

    - `/open`

      Tentatively remove an item from the queue. The item is returned as usual
      but is also set aside in case the client disappears before sending a
      "close" request. (See "Reliable Reads" below.)

    - `/close`

      Close any existing open read. (See "Reliable Reads" below.)

    - `/abort`

      Cancel any existing open read, returing that item to the head of the
      queue. It will be the next item fetched. (See "Reliable Reads" below.)

    - `/peek`

      Return the first available item from the queue, if there is one, but don't
      remove it. You can't combine this with any of the reliable read options.

  For example, to open a new read, waiting up to 500msec for an item:

        GET work/t=500/open

  Or to close an existing read and open a new one:

        GET work/close/open

- `DELETE <queue-name>`

  Drop a queue, discarding any items in it, and deleting any associated
  journal files.

- `FLUSH <queue-name>`

  Discard all items remaining in this queue. The queue remains live and new
  items can be added. The time it takes to flush will be linear to the current
  queue size, and any other activity on this queue will block while it's being
  flushed.

- `FLUSH_ALL`

  Discard all items remaining in all queues. The queues are flushed one at a
  time, as if kestrel received a `FLUSH` command for each queue.

- `VERSION`

  Display the kestrel version in a way compatible with memcache.

- `SHUTDOWN`

  Cleanly shutdown the server and exit.

- `STATS`

  Display server stats in memcache style. They're described below.

- `MONITOR <queue-name> <seconds> [max-items]`

  Monitor a queue for a time, fetching any new items that arrive. Clients
  are queued in a fair fashion, per-item, so many clients may monitor a
  queue at once. After the given timeout, a separate `END` response will
  signal the end of the monitor period. Any fetched items are open
  transactions (see "Reliable Reads" below), and should be closed with
  `CONFIRM`.

- `CONFIRM <queue-name> <count>`

  Confirm receipt of `count` items from a queue. Usually this is the response
  to a `MONITOR` command, to confirm the items that arrived during the monitor
  period.

`DUMP_CONFIG`, `DUMP_STATS`, and `RELOAD` have been removed in 3.0. Please use
the ostrich web port (2223) for formatted server stats.


Reliable reads
--------------

Normally when a client removes an item from the queue, kestrel immediately
discards the item and assumes the client has taken ownership. This isn't
always safe, because a client could crash or lose the network connection
before it gets the item. So kestrel also supports a "reliable read" that
happens in two stages, using the `/open` and `/close` options to `GET`.

When `/open` is used, and an item is available, kestrel will remove it from
the queue and send it to the client as usual. But it will also set the item
aside. If a client disconnects while it has an open read, the item is put back
into the queue, at the head, so it will be the next item fetched. Only one
item can be "open" per client connection.

A previous open request is closed with `/close`. The server will reject any
attempt to open another read when one is already open, but it will ignore
`/close` if there's no open request, so that you can add `/close` to every
`GET` request for convenience.

If for some reason you want to abort a read without disconnecting, you can use
`/abort`. But because aborted items are placed back at the head of the queue,
this isn't a good way to deal with client errors. Since the error-causing item
will always be the next one available, you'll end up bouncing the same item
around between clients instead of making progress.

There's always a trade-off: either potentially lose items or potentially
receive the same item multiple times. Reliable reads choose the latter option.
To use this tactic successfully, work items should be idempotent, meaning the
work could be done 2 or 3 times and have the same effect as if it had been
done only once (except wasting some resources).

Example:

    GET dirty_jobs/close/open
    (receives job 1)
    GET dirty_jobs/close/open
    (closes job 1, receives job 2)
    ...etc...


Server stats
------------

Global stats reported by kestrel (via the memcache `STATS` command) are:

- `uptime` - seconds the server has been online
- `time` - current time in unix epoch
- `version` - version string, like "1.2"
- `curr_items` - total of items waiting in all queues
- `total_itmes` - total of items that have ever been added in this server's
  lifetime
- `bytes` - total byte size of items waiting in all queues
- `curr_connections` - current open connections from clients
- `total_connections` - total connections that have been opened in this
  server's lifetime
- `cmd_get` - total `GET` requests
- `cmd_set` - total `SET` requests
- `cmd_peek` - total `GET/peek` requests
- `get_hits` - total `GET` requests that received an item
- `get_misses` - total `GET` requests on an empty queue
- `bytes_read` - total bytes read from clients
- `bytes_written` - total bytes written to clients
- `queue_creates` - total number of queues created
- `queue_deletes` - total number of queues deleted (includes expires)
- `queue_expires` - total number of queues expires

For each queue, the following stats are also reported:

- `items` - items waiting in this queue
- `bytes` - total byte size of items waiting in this queue
- `total_items` - total items that have been added to this queue in this
  server's lifetime
- `logsize` - byte size of the queue's journal file
- `expired_items` - total items that have been expired from this queue in this
  server's lifetime
- `mem_items` - items in this queue that are currently in memory
- `mem_bytes` - total byte size of items in this queue that are currently in
  memory (will always be less than or equal to `max_memory_size` config for
  the queue)
- `age` - time, in milliseconds, that the last item to be fetched from this
  queue had been waiting; that is, the time between `SET` and `GET`; if the
  queue is empty, this will always be zero
- `discarded` - number of items discarded because the queue was too full
- `waiters` - number of clients waiting for an item from this queue (using
  `GET/t`)
- `open_transactions` - items read with `/open` but not yet confirmed
- `total_flushes` - total number of times this queue has been flushed
- `age_msec` - age of the last item read from the queue
- `create_time` - the time that the queue was created (in milliseconds since epoch)

More detailed statistics, including latency timing measurements, are available
via the ostrich web port. To see them in human-readable form, use:

    $ curl localhost:2223/stats.txt

Similarly, in json, with the counters delta'd every minute:

    $ curl localhost:2223/stats.json\?period=60

See the ostrich documentation for more details.


Kestrel as a library
--------------------

The reusable parts of kestrel have been pulled out into a separate library -- check it out!

https://github.com/robey/libkestrel
