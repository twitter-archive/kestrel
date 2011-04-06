
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
the included [guide](docs/guide.md).

Kestrel has a mailing list here: <kestrel-talk@googlegroups.com>
http://groups.google.com/group/kestrel-talk


Features
--------

Kestrel is:

- fast

  It runs on the JVM so it can take advantage of the hard work people have
  put into java performance.

- small

  Currently about 2K lines of scala (including comments), because it relies
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
  but kestrel does not support grouping multiple operations into an atomic
  unit.


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

You can run kestrel by hand via:

    $ java -jar ./dist/kestrel-VERSION/kestrel-VERSION.jar

To run in development mode (using `development.conf` instead of
`production.conf`), add a `stage` variable:

    $ java -Dstage=development -jar ./dist/kestrel-VERSION/kestrel-VERSION.jar

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
guide). Scala docs for the config variables are here:
http://robey.github.com/kestrel/doc/main/api/net/lag/kestrel/config/KestrelConfig.html


Performance
-----------

((------FIXME------))

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
