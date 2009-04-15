
Kestrel
=======

Kestrel is a port of Blaine Cook's "starling" message queue
system from ruby to scala: <http://rubyforge.org/projects/starling/>

In Blaine's words:

> Starling is a powerful but simple messaging server that enables reliable 
> distributed queuing with an absolutely minimal overhead. It speaks the
> MemCache protocol for maximum cross-platform compatibility. Any language
> that speaks MemCache can take advantage of Starling's queue facilities.

The concept of starling is to have a single server handle reliable, ordered
message queues. When you put a cluster of these servers together,
*with no cross communication*, and pick a server at random whenever you do a
`set` or `get`, you end up with a reliable, *loosely ordered* message queue.

In many situations, loose ordering is sufficient. Dropping the requirement on
cross communication makes it horizontally scale to infinity and beyond: no
multicast, no clustering, no "elections", no coordination at all. No talking!
Shhh!

Kestrel adds several additional features, like ginormous queues, reliable
fetch, and blocking/timeout fetch -- as well as the scalability offered by
actors and the JVM.

Features
--------

Kestrel is:

- fast

  It runs on the JVM so it can take advantage of the hard work people have
  put into java performance.
  
- small

  Currently about 1.5K lines of scala (including comments), because it relies
  on Apache Mina (a rough equivalent of Danger's ziggurat or Ruby's
  EventMachine) and actors -- and frankly because Scala is extremely
  expressive.

- durable

  Queues are stored in memory for speed, but logged into a journal on disk
  so that servers can be shutdown or moved without losing any data.

- reliable

  A client can ask to "tentatively" fetch an item from a queue, and if that
  client disconnects from kestrel before confirming ownership of the item,
  the item is handed to another client. In this way, crashing clients don't
  cause lost messages.

Anti-Features
-------------

Kestrel is not:

- strongly ordered

  While each queue is strongly ordered on each machine, a cluster will
  appear "loosely ordered" because clients pick a machine at random for
  each operation. The end result should be "mostly fair".

- transactional

  This is not a database. Item ownership is transferred with acknowledgement,
  but kestrel does not concern itself with what happens to an item after a
  client has accepted it.


Use
---

Kestrel requires java 6 (for JMX support) and ant 1.7.

Building from source is easy:

    $ ant
    
Scala libraries and dependencies will be downloaded from maven repositories
the first time you do a build. The finished distribution will be in `dist`.

A sample startup script is included, or you may run the jar directly. All
configuration is loaded from `kestrel.conf`.

The created file `kestrel-VERSION.zip` can be expanded into a place like
`/usr/local` (or wherever you like) and executed within its own folder as a
self-contained package. All dependent jars are included, and the startup
script loads things from relative paths.

The default configuration puts logfiles into `/var/log/kestrel/` and queue
journal files into `/var/spool/kestrel/`.


Configuration
-------------

All of the per-queue configuration can be set in the global scope of
`kestrel.conf`, as a default for all queues, or in the per-queue configuration
to override the defaults for a specific queue. You can see an example of this
in the config file included.

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

- `max_memory_size` (128MB)

  If a queue's contents grow past this size, only this part will be kept in
  memory. Newly added items will be written directly to the journal file and
  read back into memory as the queue is drained. This setting is a release
  valve to keep a backed-up queue from consuming all memory. The value must
  be given in bytes.

- `max_age` (0 = off)

  Expiration time (in seconds) for items on this queue. Any item that has
  been sitting on the queue longer than this amount will be discarded.
  Clients may also attach an expiration time when adding items to a queue,
  but if the expiration time is longer than `max_age`, `max_age` will be
  used instead.

Performance
-----------

All of the below timings are on my 2GHz 2006-model macbook pro.

Since starling uses eventmachine in a single-thread single-process form, it
has similar results for all access types (and will never use more than one
core).

    =========  =================  ==========
    # Clients  Pushes per client  Total time
    =========  =================  ==========
            1             10,000        3.8s
           10              1,000        2.9s
          100                100        3.1s
    =========  =================  ==========

Kestrel uses N+1 I/O processor threads (where N = the number of available CPU
cores), and a pool of worker threads for handling actor events. Therefore it
handles more poorly for small numbers of heavy-use clients, and better for
large numbers of clients.

    =========  =================  ==========
    # Clients  Pushes per client  Total time
    =========  =================  ==========
            1             10,000        3.8s
           10              1,000        2.4s
          100                100        1.6s
    =========  =================  ==========

A single-threaded set of 5 million puts gives a fair idea of throughput
distribution, this time on a 2.5GHz 2008-model macbook pro:

    $ ant -f tests.xml put-many-1 -Ditems=5000000
    [java] Finished in 1137250 msec (227.5 usec/put throughput).
    [java] Transactions: min=106.00; max=108581.00 91335.00 60721.00; median=153.00; average=201.14 usec
    [java] Transactions distribution: 5.00%=129.00 10.00%=134.00 25.00%=140.00 50.00%=153.00 75.00%=177.00 90.00%=251.00 95.00%=345.00 99.00%=586.00 99.90%=5541.00 99.99%=26910.00

This works out to about 3.23MB/sec (over loopback) and about 4400 puts/sec.


Robey Pointer <<robeypointer@gmail.com>>
