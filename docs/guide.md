
An operational guide to kestrel
===============================

Kestrel is a very simple message queue that runs on the JVM and uses the
memcache protocol (with some extensions) to talk to clients.

A single kestrel server has a set of queues identified by a name, which is
also the filename of that queue's journal file (usually in
`/var/spool/kestrel`). Each queue is a strictly-ordered FIFO of "items" of
binary data. Usually this data is in some serialized format like JSON or
ruby's marshal format.

A cluster of kestrel servers is like a memcache cluster: the servers don't
know about each other, and don't do any cross-communication, so you can add as
many as you like. Clients have a list of all servers in the cluster, and pick
one at random for each operation. In this way, each queue appears to be spread
out across every server in a loose ordering.


Configuration
-------------

All of the per-queue configuration can be set in the global scope of
`production.conf`, as a default for all queues, or in the per-queue
configuration to override the defaults for a specific queue. You can see an
example of this in the default config file.

To confirm the current configuration of each queue, send "dump_config" to
a server (which can be done over telnet).

To reload the config file on a running server, send "reload" the same way.
You should immediately see the changes in "dump_config", to confirm.

- `max_items` (infinite)

  Set a hard limit on the number of items this queue can hold. When the queue
  is full, `discard_old_when_full` dictates the behavior when a client
  attempts to add another item.

- `max_size` (infinite)

  Set a hard limit on the number of bytes (of data in queued items) this
  queue can hold. When the queue is full, `discard_old_when_full` dictates
  the behavior when a client attempts to add another item.

- `discard_old_when_full` (false)

  If this is false, when a queue is full, clients attempting to add another
  item will get an error. No new items will be accepted. If this is true, old
  items will be discarded to make room for the new one. This settting has no
  effect unless at least one of `max_items` or `max_size` is set.

- `journal` (true)

  If false, don't keep a journal file for this queue. When kestrel exits, any
  remaining contents in the queue will be lost.

- `sync_journal` (false)

  If true, sync the journal file on disk after each write. This is usually
  not necessary but is available for the paranoid. It will probably reduce
  the maximum throughput of the server.

- `max_journal_size` (16MB)

  When a journal reaches this size, it will be rolled over to a new file as
  soon as the queue is empty. The value must be given in bytes.

- `max_journal_overflow` (10)

  If a journal file grows to this many times its desired maximum size, and
  the total queue contents (in bytes) are smaller than the desired maximum
  size, the journal file will be rewritten from scratch, to avoid using up
  all disk space. For example, using the default `max_journal_size` of 16MB
  and `max_journal_overflow` of 10, if the journal file ever grows beyond
  160MB (and the queue's contents are less than 16MB), the journal file will
  be re-written.

- `max_journal_size_absolute` (infinite)

  When a journal reaches this size, it will be rolled over to a new file
  *no matter how big the queue is*. The value must be given in bytes.

- `max_memory_size` (128MB)

  If a queue's contents grow past this size, only this part will be kept in
  memory. Newly added items will be written directly to the journal file and
  read back into memory as the queue is drained. This setting is a release
  valve to keep a backed-up queue from consuming all memory. The value must
  be given in bytes.

- `max_age` (0 = off)

  Expiration time (in milliseconds) for items on this queue. Any item that
  has been sitting on the queue longer than this amount will be discarded.
  Clients may also attach an expiration time when adding items to a queue,
  but if the expiration time is longer than `max_age`, `max_age` will be
  used instead.


The journal file
----------------

The journal file is the only on-disk storage of a queue's contents, and it's
just a sequential record of each add or remove operation that's happened on
that queue. When kestrel starts up, it replays each queue's journal to build
up the in-memory queue that it uses for client queries.

The journal file is rotated in one of three conditions:

1. the queue is empty and the journal is larger than `max_journal_size`

2. the queue is smaller than `max_journal_size` but the journal is larger
   than `max_journal_overflow` times `max_journal_size`

3. the journal is larger than `max_journal_size_absolute`

For example, if `max_journal_size` is 16MB (the default), and
`max_journal_overflow` is 10 (also the default), then if the queue is empty
and the journal is larger than 16MB, it will be rotated into a new (empty)
file. If the queue is smaller than 16MB, but the journal is larger than 160MB,
the journal will be rotated to contain just the live items.

You can turn the journal off for a queue (`journal` = false) and the queue
will exist only in memory. If the server restarts, all enqueued items are
lost. You can also force a queue's journal to be sync'd to disk after every
write operation (`sync_journal` = true) at a performance cost.

If a queue grows past `max_memory_size` bytes (128MB by default), only the
first 128MB is kept in memory. The journal is used to track later items, and
as items are removed, the journal is played forward to keep 128MB in memory.
This is usually known as "read-behind" mode, but Twitter engineers sometimes
refer to it as the "square snake" because of the diagram used to brainstorm
the implementation. When a queue is in read-behind mode, removing an item will
often cause 2 disk operations instead of one: one to record the remove, and
one to read an item in from disk to keep 128MB in memory. This is the
trade-off to avoid filling memory and crashing the JVM.



-- expiration of items

-- fanout queues

-- non-memcache commands (dump_config, flush)





