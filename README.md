
Kestrel
=======

Kestrel is based on Blaine Cook's "starling" simple, distributed message
queue, with added features and bulletproofing, as well as the scalability
offered by actors and the JVM.

Each server handles a set of reliable, ordered message queues. When you put a
cluster of these servers together, *with no cross communication*, and pick a
server at random whenever you do a `put` or `get`, you end up with a reliable,
*loosely ordered* message queue.

In many situations, loose ordering is sufficient. Dropping the requirement on
cross communication makes it horizontally scale to infinity and beyond: no
multicast, no clustering, no "elections", no coordination at all. No talking!
Shhh!

For more information about what it is and how to use it, check out
the included [guide](https://github.com/robey/kestrel/blob/master/docs/guide.md).

Kestrel has a mailing list here:
[kestrel-talk@googlegroups.com](http://groups.google.com/group/kestrel-talk)

Author's address: Robey Pointer \<robeypointer@gmail.com>


Features
--------

Kestrel is:

- fast

    It runs on the JVM so it can take advantage of the hard work people have
    put into java performance.

- small

    Currently about 2500 lines of scala, because it relies on
    [finagle](https://github.com/twitter.finagle) and
    [netty](http://www.jboss.org/netty), and because Scala is extremely
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
    so no jobs will be lost, but kestrel does not support grouping multiple
    operations into an atomic unit.


Downloading it
--------------

The latest release is always on the homepage here:

- [http://robey.github.com/kestrel](http://robey.github.com/kestrel)

Or the latest development versions & branches are on github:

- [http://gitub.com/robey/kestrel](http://github.com/robey/kestrel)


Building it
-----------

Kestrel requires java 6 and sbt 0.7.4. On OS X 10.5, you may have to hard-code
an annoying `JAVA_HOME` to use java 6:

    $ export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home

Building from source is easy:

    $ sbt clean update package-dist

Scala libraries and dependencies will be downloaded from maven repositories
the first time you do a build. The finished distribution will be in `dist`.


Running it
----------

You can run kestrel by hand, in development mode, via:

    $ ./dist/kestrel-VERSION/scripts/devel.sh

Like all ostrich-based servers, it uses the "stage" property to determine
which config file to load, so `devel.sh` sets `-Dstage=development`.

When running it as a server, a startup script is provided in
`dist/kestrel-VERSION/scripts/kestrel.sh`. The script assumes you have
`daemon`, a standard daemonizer for Linux, but also available
[here](http://libslack.org/daemon/) for all common unix platforms.

The created archive `kestrel-VERSION.tar.bz2` can be expanded into a place
like `/usr/local` (or wherever you like) and executed within its own folder as
a self-contained package. All dependent jars are included, and the startup
script loads things from relative paths.

The default configuration puts logfiles into `/var/log/kestrel/` and queue
journal files into `/var/spool/kestrel/`.

The startup script logs extensive GC information to a file named `stdout` in
the log folder. If kestrel has problems starting up (before it can initialize
logging), it will usually appear in `error` in the same folder.


Configuration
-------------

Queue configuration is described in detail in `docs/guide.md` (an operational
guide). Scala docs for the config variables are
[here](http://robey.github.com/kestrel/doc/main/api/net/lag/kestrel/config/KestrelConfig.html).


Performance / Load
------------------

Several performance/load tests are included. To run them, first start up a
kestrel instance locally.

    $ sbt clean update package-dist
    $ ./dist/kestrel-VERSION/scripts/devel.sh

All of the tests can be run from scripts in `scripts/load/`, and all of them
respond to "`--help`".

## Put-many

This test just spams a kestrel server with "put" operations, to see how
quickly it can absorb and journal them.

A sample run (on a Mac laptop):

    $ ./dist/kestrel/scripts/load/put-many -n 100000
    Flushing queues first.
    Put 100000 items of 1024 bytes in bursts of 1 to localhost:22133 in 1
      queues named spam using 100 clients.
    Finished in 4410 msec (44.1 usec/put throughput).
    Distribution in usec: min=0 max=3858 p50=1 p75=1 p90=1 p95=2 p99=10
      p999=316 p9999=472

## Many-clients

This test has one producer that trickles out one item at a time, and a pile of
consumers fighting for each item. It usually takes exactly as long as the
number of items times the delay, but is useful as a validation test to make
sure kestrel works as advertised without blowing up.

A sample run:

    $ ./dist/kestrel/scripts/load/many-clients
    many-clients: 100 items to localhost using 100 clients, kill rate 0%, at
      100 msec/item
    Flushing queues first.
    Received 100 items in 12138 msec.

This test always takes over 10 seconds -- it's a load test instead of a
speed test.

## Flood

This test starts up one producer and one consumer, and just floods items
through kestrel as fast as it can.

A sample run:

    $ ./dist/kestrel/scripts/load/flood
    Flushing queues first.
    flood: producers=1 consumers=1 each sending 10000 items of 1kB through
      spam
    Finished in 884 msec (88.4 usec/put throughput).
    Consumer(s) spun 6 times in misses.

## Packing

This test starts up one producer and one consumer, seeds the queue with a
bunch of items to cause it to fall behind, then does cycles of flooding items
through the queue, separated by pauses. It's meant to test kestrel's behavior
with a queue that's fallen behind and *stays* behind indefinitely, to make
sure the journal files are cleaned up periodically without affecting
performance too badly.

(In the past, journals were "packed" as they fell behind. These days, the old
journal files are just erased as their items are read, so the name of this
test is historic and a bit misleading.)

A sample run:

    $ ./dist/kestrel/scripts/load/packing -c 10 -q small
    packing: 25000 items of 1kB with 1 second pauses
    Flushing queues first.
    Wrote 25000 items starting at 0.
    cycle: 1
    Wrote 25000 items starting at 25000.
    Read 25000 items in 2862 msec. Consumer spun 0 times in misses.
    cycle: 2
    Wrote 25000 items starting at 50000.
    Read 25000 items in 2426 msec. Consumer spun 0 times in misses.
    ...
    cycle: 10
    Wrote 25000 items starting at 250000.
    Read 25000 items in 2464 msec. Consumer spun 0 times in misses.
    Read 25000 items in 2181 msec. Consumer spun 0 times in misses.

You can see the journals being built and erased in the kestrel log. Like
"many-clients", this test is a load test instead of a speed test.

## Leaky-reader

This test starts a producer and several consumers, with the consumers
occasionally "forgetting" to acknowledge an item that they've read. It
verifies that the un-acknowledged items are eventually handed off to another
consmer.

A sample run:

    $ ./dist/kestrel/scripts/load/leaky-reader -n 100000 -t 10
    leaky-reader: 10 threads each sending 100000 items through spam
    Flushing queues first.
    1000
    2000
    100000
    Finished in 40220 msec (40.2 usec/put throughput).
    Completed all reads

Like "many-clients", it's just a load test.
