
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

Building from source is easy:

    $ ant
    
Scala libraries and dependencies will be downloaded from maven repositories
the first time you do a build. The finished distribution will be in `dist`.

A sample startup script is included, or you may run the jar directly. All
configuration is loaded from `kestrel.conf`.


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


Robey Pointer <<robeypointer@gmail.com>>
