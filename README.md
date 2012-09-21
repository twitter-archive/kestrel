
Kestrel
=======

Kestrel is based on Blaine Cook's "starling" simple, distributed message
queue, with added features and bulletproofing, as well as the scalability
offered by actors and the JVM.

Each server handles a set of reliable, ordered message queues. When you put a
cluster of these servers together, *with no cross communication*, and pick a
server at random whenever you do a `set` or `get`, you end up with a reliable,
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

    Currently about 2500 lines of scala, because it relies on Netty (a rough
    equivalent of Danger's ziggurat or Ruby's EventMachine) -- and because
    Scala is extremely expressive.

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
    but kestrel does not support grouping multiple operations into an atomic
    unit.


Downloading it
--------------

The latest release is always on the homepage here:

- [http://robey.github.com/kestrel](http://robey.github.com/kestrel)

Or the latest development versions & branches are on github:

- [http://gitub.com/robey/kestrel](http://github.com/robey/kestrel)


Building it
-----------

Kestrel requires java 6 and sbt 0.11.2. Presently some sbt plugins used by kestrel
depend on that exact version of sbt. On OS X 10.5, you may have to hard-code
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

The created archive `kestrel-VERSION.zip` can be expanded into a place
like `/usr/local` (or wherever you like) and executed within its own folder as
a self-contained package. All dependent jars are included. The current startup
script, however, assumes that kestrel has been deployed to
`/usr/local/kestrel/current` (e.g., as if by capistrano), and the startup
script loads kestrel from that path.

The default configuration puts logfiles into `/var/log/kestrel/` and queue
journal files into `/var/spool/kestrel/`.

The startup script logs extensive GC information to a file named `stdout` in
the log folder. If kestrel has problems starting up (before it can initialize
logging), it will usually appear in `error` in the same folder.


Configuration
-------------

Queue configuration is described in detail in `docs/guide.md` (an operational
guide). Scala docs for the config variables are
[here](http://robey.github.com/kestrel/api/main/api/net/lag/kestrel/config/KestrelConfig.html).


Performance
-----------

Several performance tests are included. To run them, first start up a kestrel instance
locally.

    $ sbt clean update package-dist
    $ ./dist/kestrel-VERSION/scripts/devel.sh

## Put-many

This test just spams a kestrel server with "put" operations, to see how
quickly it can absorb and journal them.

A sample run on a 2010 MacBook Pro:

    $ ./dist/kestrel/scripts/load/put-many -n 100000
    Put 100000 items of 1024 bytes to localhost:22133 in 1 queues named spam
      using 100 clients.
    Finished in 6137 msec (61.4 usec/put throughput).
    Transactions: min=71.00; max=472279.00 472160.00 469075.00;
      median=3355.00; average=5494.69 usec
    Transactions distribution: 5.00%=485.00 10.00%=1123.00 25.00%=2358.00
      50.00%=3355.00 75.00%=4921.00 90.00%=7291.00 95.00%=9729.00
      99.00%=50929.00 99.90%=384638.00 99.99%=467899.00

## Many-clients

This test has one producer that trickles out one item at a time, and a pile of
consumers fighting for each item. It usually takes exactly as long as the
number of items times the delay, but is useful as a validation test to make
sure kestrel works as advertised without blowing up.

A sample run on a 2010 MacBook Pro:

    $ ./dist/kestrel/scripts/load/many-clients
    many-clients: 100 items to localhost using 100 clients, kill rate 0%,
      at 100 msec/item
    Received 100 items in 11046 msec.

This test always takes about 11 seconds -- it's a load test instead of a
speed test.

## Flood

This test starts up one producer and one consumer, and just floods items
through kestrel as fast as it can.

A sample run on a 2010 MacBook Pro:

    $ ./dist/kestrel/scripts/load/flood
    flood: 1 threads each sending 10000 items of 1kB through spam
    Finished in 1563 msec (156.3 usec/put throughput).
    Consumer(s) spun 0 times in misses.

## Packing

This test starts up one producer and one consumer, seeds the queue with a
bunch of items to cause it to fall behind, then does cycles of flooding items
through the queue, separated by pauses. It's meant to test kestrel's behavior
with a queue that's fallen behind and *stays* behind indefinitely, to make
sure the journal files are packed periodically without affecting performance
too badly.

A sample run on a 2010 MacBook Pro:

    $ ./dist/kestrel/scripts/load/packing -c 10 -q small
    packing: 25000 items of 1kB with 1 second pauses
    Wrote 25000 items starting at 0.
    cycle: 1
    Wrote 25000 items starting at 25000.
    Read 25000 items in 5279 msec. Consumer spun 0 times in misses.
    cycle: 2
    Wrote 25000 items starting at 50000.
    Read 25000 items in 4931 msec. Consumer spun 0 times in misses.
    ...
    cycle: 10
    Wrote 25000 items starting at 250000.
    Read 25000 items in 5304 msec. Consumer spun 0 times in misses.
    Read 25000 items in 3370 msec. Consumer spun 0 times in misses.

You can see the journals being packed in the kestrel log. Like
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
