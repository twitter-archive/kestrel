
A working guide to kestrel
==========================

Kestrel is a very simple message queue that runs on the JVM. It supports
multiple protocols:

- memcache: the memcache protocol, with some extensions
- thrift: Apache Thrift-based RPC
- text: a simple text-based protocol

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
reserved for future use. Queue names are case-sensitive, but if you're running
kestrel on OS X or Windows, you will want to refrain from taking advantage of
this, since the journal filenames on those two platforms are *not*
case-sensitive.

A cluster of kestrel servers is like a memcache cluster: the servers don't
know about each other, and don't do any cross-communication, so you can add as
many as you like. The simplest clients have a list of all servers in the
cluster, and pick one at random for each operation. In this way, each queue
appears to be spread out across every server, with items in a loose ordering.
More advanced clients can find kestrel servers via ZooKeeper.

When kestrel starts up, it scans the journal folder and creates queues based
on any journal files it finds there, to restore state to the way it was when
it last shutdown (or was killed or died). New queues are created by referring
to them (for example, adding or trying to remove an item). A queue can be
deleted with the "delete" command.


Configuration
-------------

**NOTE:** Kestrel 2.3.4 introduces inheritance for queue configurations. For more
information, see below.

The config files for kestrel are scala expressions loaded at runtime, usually
from `production.scala`, although you can use `development.scala` by passing
`-Dstage=development` to the java command line.

The config file evaluates to a `KestrelConfig` object that's used to configure
the server as a whole, a default queue, and any overrides for specific named
queues. The fields on `KestrelConfig` are documented here with their default
values:
[KestrelConfig](http://robey.github.com/kestrel/api/main/api/net/lag/kestrel/config/KestrelConfig.html)

To confirm the current configuration of each queue, send "dump_config" to
a server (which can be done over telnet).

To reload the config file on a running server, send "reload" the same way.
You should immediately see the changes in "dump_config", to confirm. Reloading
will only affect queue and alias configuration, not global server configuration.
To change the server configuration, restart the server.

Logging is configured according to `util-logging`. The logging configuration
syntax is described here:
[util-logging](https://github.com/twitter/util/blob/master/util-logging/README.markdown)

Per-queue configuration options are documented here:
[QueueBuilder](http://robey.github.com/kestrel/api/main/api/net/lag/kestrel/config/QueueBuilder.html)

Queue alias configuration options are documented here:
[AliasBuilder](http://robey.github.com/kestrel/api/main/api/net/lag/kestrel/config/AliasBuilder.html)

Configuration Changes Starting in Kestrel 2.3.4
-----------------------------------------------

Starting with Kestrel 2.3.4, queue configurations are inherited:

* Any queue with no explict configuration (see `queues` in `KestrelConfig`) uses the default
  queue configuration (see `default` in `KestrelConfig`). This behavior is unchanged from
  previous versions.
* Any master (e.g. not fanout) queue with a queue configuration overrides the default queue
  configuration. For example, if `default.maxMemorySize` is set, all explicitly configured
  queues will inherit that setting *unless* explicitly overridden in the queue's configuration.
  Older versions of Kestrel *did not* apply values from the default queue configuration to any
  explicitly configured queue.
* Any fanout queue (e.g., a queue with a `+` in its name), inherits its master queue's
  configuration, unless explicitly overridden (see `queues` in `KestrelConfig`). Older versions
  of Kestrel silently ignored explicit fanout queue configurations.

### Example Configuration
-------------------------

Existing configurations should continue to load, but the resulting configuration may
differ. As an example, the following configuration file and table illustrate the differences
between a configuration loaded by Kestrel 2.3.3 and Kestrel 2.3.4 (and later).

    new KestrelConfig {
       default.maxMemorySize = 8.megabytes

       queues = new QueueBuilder() {
           name = "q"
           maxItems = 500
       } :: new QueueBuilder() {
           name = "q+fanout"
           maxAge = 1.minute
       } :: new QueueBuilder() {
           name = "x"
           maxMemorySize = 16.megabytes
       }
    }


<table>
  <tr><th>Queue</th>    <th>Setting</th>       <th>Kestrel <= 2.3.3</th> <th>Kestrel >= 2.3.4</th>   </tr>
  <tr><td>q</td>        <td>maxMemorySize</td> <td>128.megabytes</td>    <td>8.megabytes</td>        </tr>
  <tr><td>q+fanout</td> <td>maxMemorySize</td> <td>128.megabytes</td>    <td>8.megabytes</td>        </tr>
  <tr><td>x</td>        <td>maxMemorySize</td> <td>16.megabytes</td>     <td>16.megabytes</td>       </tr>
  <tr><td>q</td>        <td>maxItems</td>      <td>500</td>              <td>500</td>                </tr>
  <tr><td>q+fanout</td> <td>maxItems</td>      <td>500</td>              <td>500</td>                </tr>
  <tr><td>q+fanout</td> <td>maxAge</td>        <td>None</td>             <td>Some(1.minute)</td>     </tr>
</table>


Full queues
-----------

A queue can have the following limits set on it:

- `maxItems` - total items in the queue
- `maxSize` - total bytes of data in the items in the queue

If either of these limits is reached, no new items can be added to the queue.
(Clients will receive an error when trying to add.) If you set
`discardOldWhenFull` to true, then all adds will succeed, and the oldest
item(s) will be silently discarded until the queue is back within the item
and size limits.

`maxItemSize` limits the size of any individual item. If an add is attempted
with an item larger than this limit, it always fails.


The journal file
----------------

The journal file is the only on-disk storage of a queue's contents, and it's
just a sequential record of each add or remove operation that's happened on
that queue. When kestrel starts up, it replays each queue's journal to build
up the in-memory queue that it uses for client queries.

The journal file is rotated in one of two conditions:

1. the queue is empty and the journal is larger than `defaultJournalSize`

2. the journal is larger than `maxJournalSize`

For example, if `defaultJournalSize` is 16MB (the default), then if the queue
is empty and the journal is larger than 16MB, it will be truncated into a new
(empty) file. If the journal is larger than `maxJournalSize` (1GB by default),
the journal will be rewritten periodically to contain just the live items.

You can turn the journal off for a queue (`keepJournal` = false) and the queue
will exist only in memory. If the server restarts, all enqueued items are
lost. You can also force a queue's journal to be sync'd to disk periodically,
or even after every write operation, at a performance cost, using
`syncJournal`.

If a queue grows past `maxMemorySize` bytes (128MB by default), only the
first 128MB is kept in memory. The journal is used to track later items, and
as items are removed, the journal is played forward to keep 128MB in memory.
This is usually known as "read-behind" mode, but Twitter engineers sometimes
refer to it as the "square snake" because of the diagram used to brainstorm
the implementation. When a queue is in read-behind mode, removing an item will
often cause 2 disk operations instead of one: one to record the remove, and
one to read an item in from disk to keep 128MB in memory. This is the
trade-off to avoid filling memory and crashing the JVM.


Item expiration
---------------

When they come from a client, expiration times are handled in the same way as
memcache: if the number is small (less than one million), it's interpreted as
a relative number of seconds from now. Otherwise it's interpreted as an
absolute unix epoch time, in seconds since the beginning of 1 January 1970
GMT.

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

If a queue name has a `+` in it (like "`orders+audit`"), it's treated as a
fanout queue, using the format `<parent>+<child>`. These queues belong to a
parent queue -- in this example, the "orders" queue. Every item written into
a parent queue will also be written into each of its children.

Fanout queues each have their own journal file (if the parent queue has a
journal file) and otherwise behave exactly like any other queue. You can get
and peek and even add items directly to a child queue if you want. It uses the
parent queue's configuration instead of having independent child queue
configuration blocks.

When a fanout queue is first referenced by a client, the journal file (if any)
is created, and it will start receiving new items written to the parent queue.
Existing items are not copied over. A fanout queue can be deleted to stop it
from receiving new items.

`fanoutOnly` may be set to true if the queue in question will only serve write
point for fanout queues.  No journal file will be kept for the parent, only
for the child queues.  This saves the overhead of writing to the parent and
removes the need to empty it.  Note that setting `fanoutOnly` to true and
having no fanouts for the queue effectively makes it a black hole.

Queue Aliases
-------------

Queue aliases are somewhat similar to fanout queues, but without a required
naming convention or implicit creation of child queues. A queue alias can
only be used in set operations. Kestrel responds to attempts to retrieve
items from the alias as if it were an empty queue. Delete and flush requests
are also ignored.


Protocols
---------

Kestrel supports three protocols: memcache, thrift and text. The
[Finagle project](http://twitter.github.com/finagle/) can be used to connect clients
to a Kestrel server via the memcache or thrift protocols.

### Thrift
----------

The thrift protocol is documented in the thrift IDL:
[kestrel.thrift](https://github.com/robey/kestrel/blob/master/src/main/thrift/kestrel.thrift)

Reliable reads via the thrift protocol are specified by indicating how long the server
should wait before aborting the unacknowledged read.


### Memcache
------------

The official memcache protocol is described here:
[protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)

The kestrel implementation of the memcache protocol commands is described below.

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

- `RELOAD`

  Reload the config file and reconfigure all queues. This should have no
  noticable effect on the server's responsiveness.

- `STATS`

  Display server stats in memcache style. They're described below.

- `DUMP_STATS`

  Display server stats in a more readable style, grouped by queue. They're
  described below.

- `MONITOR <queue-name> <seconds> [max-items]`

  Monitor a queue for a time, fetching any new items that arrive, up to an
  optional maximum number of items. Clients are queued in a fair fashion,
  per-item, so many clients may monitor a queue at once. After the given
  timeout, a separate `END` response will signal the end of the monitor
  period. Any fetched items are open transactions (see "Reliable Reads"
   below), and should be closed with `CONFIRM`.

- `CONFIRM <queue-name> <count>`

  Confirm receipt of `count` items from a queue. Usually this is the response
  to a `MONITOR` command, to confirm the items that arrived during the monitor
  period.

- `STATUS`

Displays the kestrel server's current status (see section on Server Status,
below).

- `STATUS <new-status>`

Switches the kestrel server's current status to the given status (see section
on Server Status, below).


#### Reliable reads
-------------------

Note: this section is specific to the memcache protocol.

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


### Text protocol
-----------------

Kestrel supports a limited, text-only protocol. You are encouraged to use the
memcache protocol instead.

The text protocol does not support reliable reads.


Server Status
-------------

Each kestrel server maintains its current status. Normal statuses are

- `Up`: the server is available for all operations
- `ReadOnly`: the server is available for non-modifying operations only;
              commands that modify queues (set, delete, flush) are rejected as
	      errors.
- `Quiescent`: the server rejects as an error operations on any queue. One
               notable exception is transactions begun before the server entered
	       the quiesecent state may still be confirmed.

One additional status is `Down`, which is only used transiently when kestrel is
in the process of shutting down.

The server's current status is persisted (specified in
[KestrelConfig](http://robey.github.com/kestrel/api/main/api/net/lag/kestrel/config/KestrelConfig.html)).
When kestrel is restarted it automatically returns to it's previous status,
based on the value in the status file. If the status file does not exist or
cannot be read, kestrel uses a default status, also configured in KestrelConfig.

When changing from a less restrictive status to a more restrictive status
(e.g., from `Up` to `ReadOnly` or from `ReadOnly` to `Quiescent`), the
config option `statusChangeGracePeriod` determines how long kestrel will
continue to allow restricted operations to continue before it begins rejecting
them. This allows clients that are aware of the kestrel server's status a
grace period to learn the new status and cease the forbidden operations before
beginning to encounter errors.

### ZooKeeper Server Sets
-------------------------

Kestrel uses Twitter's ServerSet library to support discovery of kestrel
servers allowing a given operation. The ServerSet class is documented here:
[ServerSet](http://twitter.github.com/commons/apidocs/index.html#com.twitter.common.zookeeper.ServerSet)

If the optional `zookeeper` field of `KestrelConfig` is specified, kestrel will
attempt to use the given configuration to join a logical set of kestrel servers.
The ZooKeeper host, port and other connection options are documented here:
[ZooKeeperBuilder](http://robey.github.com/kestrel/api/main/api/net/lag/kestrel/config/ZooKeeperBuilder.html)

Kestrel servers will join 0, 1, or 2 server sets depending on their current
status. When `Up`, the server joins two server sets: one for writes and one for
reads. When `ReadOnly`, the server joins only the read set. When `Quiescent`,
the server joins no sets. ZooKeeper-aware kestrel clients can watch the
server set for changes and adjust their connections accordingly. The
`statusChangeGracePeriod` configuration option may be used to allow clients
time to detect and react to the status change before they begin receiving
errors from kestrel.

The ZooKeeper path used to register the server set
is based on the `pathPrefix` option. Kestrel automatically appends `/write` and
`/read` to distinguish the write and read sets.

Kestrel advertises all of its endpoints in each server set that it joins.
The default endpoint is memcache, if configured. The default endpoint falls
back to the thrift endpoint and then the text protocol endpoint. All three
endpoints are advertised as additional endpoints under the names `memcache`,
`thrift` and `text`.

Consider setting the  `defaultStatus` option to `Quiescent` to prevent kestrel
from prematurely advertising its status via ZooKeeper.

Installations that require additional customization of ZooKeeper credentials,
or other site-specific ZooKeeper initialization can override the
`clientInitializer` and `serverSetInitializer` options to invoke the
necessary site-specific code. The recommended implementation is to place
the site-specific code in its own JAR file, take the necessary steps to
include the JAR in kestrel's class path, and place as little logic as possible
in the kestrel configuration file.


Server stats
------------

Global stats reported by kestrel are:

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
- `transactions` - number of transactional get requests (irrespective of whether an
  item was read or not)
- `canceled_transactions` - number of transactional get requests canceled (for any
  reason)
- `total_flushes` - total number of times this queue has been flushed
- `age_msec` - age of the last item read from the queue
- `create_time` - the time that the queue was created (in milliseconds since epoch)

Statistics may be retrieved by accessing the
[Ostrich admin HTTP service](https://github.com/twitter/ostrich) on the admin HTTP port.
For example: `http://kestrel.host:2223/stats.json?period=60`.

Statistics are also available via the memcache protocol using the `STATS` command.


Kestrel as a library
--------------------

You can use kestrel as a library by just sticking the jar on your classpath.
It's a cheap way to get a durable work queue for inter-process or inter-thread
communication. Each queue is represented by a `PersistentQueue` object:

    class PersistentQueue(val name: String, persistencePath: String,
                          @volatile var config: QueueConfig, timer: Timer,
                          queueLookup: Option[(String => Option[PersistentQueue])]) {

and must be initialized before using:

    def setup(): Unit

specifying the path for the journal files (if the queue will be journaled),
the name of the queue, a `QueueConfig` object (derived from `QueueBuilder`),
a timer for handling timeout reads, and optionally a way to find other named
queues (for `expireToQueue` support).

To add an item to a queue:

    def add(value: Array[Byte], expiry: Option[Time]): Boolean

It will return `false` if the item was rejected because the queue was full.

Queue items are represented by a case class:

    case class QItem(addTime: Time, expiry: Option[Time], data: Array[Byte], var xid: Int)

and several operations exist to remove or peek at the head item:

    def peek(): Option[QItem]
    def remove(): Option[QItem]

To open a reliable read, set `transaction` true, and later confirm or unremove
the item by its `xid`:

    def remove(transaction: Boolean): Option[QItem]
    def unremove(xid: Int)
    def confirmRemove(xid: Int)

You can also asynchronously remove or peek at items using futures.

    def waitRemove(deadline: Option[Time], transaction: Boolean): Future[Option[QItem]]
    def waitPeek(deadline: Option[Time]): Future[Option[QItem]]

When done, you should close the queue:

    def close(): Unit
    def isClosed: Boolean

Here's a short example:

    var queue = new PersistentQueue("work", "/var/spool/kestrel", config, timer, None)
    queue.setup()

    // add an item with no expiration:
    queue.add("hello".getBytes, 0)

    // start to remove it, then back out:
    val item = queue.remove(true)
    queue.unremove(item.xid)

    // remove an item with a 500msec timeout, and confirm it:
    queue.waitRemove(500.milliseconds.fromNow, true)() match {
      case None =>
        println("nothing. :(")
      case Some(item) =>
        println("got: " + new String(item.data))
        queue.confirmRemove(item.xid)
    }

    queue.close()
