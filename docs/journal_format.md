
# Kestrel journal format

A kestrel queue's journal is just a sequence of operations that have happened on the queue. The
queue is assumed to be empty at the start of the journal, and doing each operation in order will
re-create the current state of the queue. (This is why the journal can be erased and start over
when the queue is empty.)

Each operation is a byte opcode followed by some optional metadata (per opcode). The first two
operations have existed since starling (in 2007), so kestrel still supports the reading of older
journals as an upgrade path, and plans to indefinitely.

Metadata ints are in little-endian format. An intro "size" field always refers to the size of the
entire block after the opcode. So, for example, an ADD of a 1-byte item would have a size of 5 to
cover the 4-byte expiration and the 1-byte data.


## Opcodes

- ADD (0)

        i32 size
        i32 expiration time (absolute, epoch time, in seconds)
        binary data of (size - 4)

  Add an item to the end of the queue.

- REMOVE (1)

  Remove an item from the head of the queue.

- ADDX (2)

        i32 size
        i64 add time (absolute, epoch time, in milliseconds)
        i64 expiration time (absolute, epoch time, in milliseconds)
        binary data of (size - 16)

  Add an item to the end of the queue.

- REMOVE_TENTATIVE (3)

  Reserve an item from the head of the queue. It should be removed from the queue,
  given the next available transaction ID (xid) and placed in a table of outstanding transactions.

- SAVE_XID (4)

        i32 xid

  Set the current (last-used) transaction ID. Usually this is at the very beginning of a journal,
  to provide starting state.

- UNREMOVE (5)

        i32 xid

  Take the identified open transaction and put it back on the head of the queue. The transaction
  was cancelled.

- CONFIRM_REMOVE (6)

        i32 xid

  Remove the identified open transaction and consider it finished.

- ADD_XID (7)

        i32 xid
        i32 size
        i64 add time (absolute, epoch time, in milliseconds)
        i64 expiration time (absolute, epoch time, in milliseconds)
        binary data of (size - 16)

  Add an item to the queue with a special, named, transaction ID. This is ususally used to
  represent transactions that were open when the journal was rewritten. (Each ADD\_XID is usually
  followed by a REMOVE\_TENTATIVE.)

- STATE_DUMP (8)

        i32 xid
        i32 number of open transactions to follow as ADD_XID

  Used by read-behind for state in rewriting the journal file. If not in read-behind,
  this really marks the end of this journal file and time to move to the next one.


## Initial operations in the journal

A journal file that has been rewritten or packed will usuall have a set of
operations at the beginning to rebuild the state of the transaction ID
("xid"), any open transactions, and the current queue contents:

- `save_xid`
- for each open transaction:
  - `add_xid`
  - `remove_tentative`
- for each queue item:
  - `addx`

The rest of the journal will be operations as they happen. Since this initial
set of operations can be treated just like any other queue operation, there's
no special treatment when the journal is replayed at startup.


## Multiple journal files

The latest journal file for a queue is named simply `(name)` with no dots in
it. For example, the latest journal file for queue "cars" is named `cars`.

As the journal is rotated, the current timestamp is appended. A rotated
journal for cars might be named `cars.904`. When recovering the journals on
restart, kestrel replays all the rotated journal files in timestamp order
(earliest first), and then the non-timestamped file last (if it exists). They
are treated exactly the same as if they were chunks of a larger, contiguous
file.

Journals with `~~` in their filename are temporary. If found on restart, they
can be ignored.

If previous rolled journal files are packed together, the state will
initially be written into a temporary file, and then renamed to end with
`.(timestamp).pack`. The timestamp means that any rotated journal with
timestamp less than or equal to the pack-file timestamp is now dead and
should be deleted. For example, if `cars.950.pack` exists, then `cars.904`
and `cars.950` should be ignored and deleted, but `cars.951` is still valid.
Normally, if it doesn't crash, the packing process will delete these older
files after creating the pack file. Then it will rename the pack file to
remove the ".pack" extension.


## Journal packing strategy

To keep from using an infinite amount of disk space, kestrel has a few
strategies for erasing or compressing old journal files.

- If the queue is empty, then after `defaultJournalSize` (usually 16MB) of
  journal is written, the file is erased and a new file is started with only
  open transactions. (This is the normal "steady-state" operation mode.)

- If the queue *is* in read-behind, a new journal file is started after each
  `maxMemorySize` (usually 128MB). At the beginning of each new file, a
  checkpoint is saved in memory with the current list of open transactions.

  When the read-behind pointer reaches one of these checkpoints, the older
  files are replaced with a much smaller file containing only:

  - the list of open transactions (from the checkpoint)
  - a list of newly opened transactions since the checkpoint
  - a count of removed items since the checkpoint
  - the items currently on the in-memory portion of the queue

The cons to this strategy are:

- Up to `maxMemorySize` (usually 128MB) will be rewritten during each
  checkpoint.
- If a kestrel has N live connections, up to N open transactions will be
  written out during each checkpoint. For 50k connections, each holding a 1KB
  transaction, that could be 50MB.

Kestrel versions prior to 2.4.1 will also attempt to compact the journal if
the queue is not in read-behind (that is, all of the queue's contents are in
memory) and the journal files total more than `maxJournalSize` (usually
1GB). In this case, the journal is rewritten from scratch. We know that
there will be no more than `maxMemorySize` (usually 128MB) worth of data to
write. This feature was removed because in the absence of remove operations,
it was possible to cause Kestrel to perform this rewrite on every add
operation, rendering it unusable. The work around for this behavior is to
increase `maxJournalSize` such that the ratio of `maxJournalSize` to
`maxMemorySize` is greater than the ratio of the `minimum item size + 21` to
the `minimum item size`.
