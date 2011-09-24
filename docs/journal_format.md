
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
