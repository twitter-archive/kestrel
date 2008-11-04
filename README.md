
Scarling
========

Scarling is a port of Blaine Cook's "starling" message queue
system from ruby to scala: <http://rubyforge.org/projects/starling/>

In Blaine's words:

> Starling is a powerful but simple messaging server that enables reliable 
> distributed queuing with an absolutely minimal overhead. It speaks the
> MemCache protocol for maximum cross-platform compatibility. Any language
> that speaks MemCache can take advantage of Starling's queue facilities.

The concept of starling is to have a single server handle reliable, ordered
message queues. When you put a cluster of these servers together, *with no
cross communication*, and pick a server at random whenever you do a `set` or
`get`, you end up with a reliable, *loosely ordered* message queue.

In many situations, loose ordering is sufficient. Dropping the requirement on
cross communication makes it horizontally scale to infinity and beyond: no
multicast, no clustering, no "elections", no coordination at all. No talking!
Shhh!

Features
--------

Scarling is:

- fast

  It runs on the JVM so it can take advantage of the hard work people have
  put into java performance.
  
- small

  Currently about 1K lines of scala (including comments), because it relies
  on Appache Mina (a rough equivalent of Danger's ziggurat or Ruby's
  eventmachine) and actors. And frankly because Scala is extremely
  expressive.

- durable

  Queues are stored in memory for speed, but logged into a journal on disk
  so that servers can be shutdown or moved without losing any data.
  
Anti-Features
-------------

Scarling is not:

- strongly ordered

  While each queue is strongly ordered on each machine, a cluster will
  appear "loosely ordered" because clients pick a machine at random for
  each operation. The end result should be "mostly fair".

- transactional

  Currently when you `get` an item from a queue, it is removed instantly
  from that queue and you are responsible for it. If a client crashes after
  getting at item, it may be lost.


Use
---

Building from source is easy:

    $ ant
    
Scala libraries and dependencies will be downloaded from maven repositories
the first time you do a build. The finished distribution will be in `dist`.

A sample startup script is included, or you may run the jar directly. All
configuration is loaded from `scarling.conf`.


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

Scarling uses N+1 I/O processor threads (where N = the number of available CPU
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
