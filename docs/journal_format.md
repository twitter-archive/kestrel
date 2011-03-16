
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
