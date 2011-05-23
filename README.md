
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
the included [guide](blob/master/docs/guide.md).

Kestrel has a mailing list here: <kestrel-talk@googlegroups.com>
http://groups.google.com/group/kestrel-talk

Author's address: Robey Pointer <<robeypointer@gmail.com>>


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

Several performance tests are included. To run them, first start up a kestrel instance
locally.

    $ sbt clean update package-dist
    $ VERSION="2.1.0-SNAPSHOT"
    $ java -server -Xmx1024m -Dstage=development -jar ./dist/kestrel-$VERSION/kestrel-$VERSION.jar

## Put-many

This test just spams a kestrel server with "put" operations, to see how
quickly it can absorb and journal them.

    $ sbt "put-many --help"
    usage: put-many [options]
        spam items into kestrel

    options:
        -c CLIENTS
            use CLIENTS concurrent clients (default: 100)
        -n ITEMS
            put ITEMS items into the queue (default: 10000)
        -b BYTES
            put BYTES per queue item (default: 1024)

A sample run on a 2010 MacBook Pro:

    [info] == put-many ==
    [info] Running net.lag.kestrel.load.PutMany -n 1000000
    Finished in 64921 msec (64.9 usec/put throughput).
    Transactions: min=95.00; max=528107.00 524847.00 521780.00;
      median=3433.00; average=5551.77 usec
    Transactions distribution: 5.00%=810.00 10.00%=1526.00 25.00%=2414.00
      50.00%=3433.00 75.00%=4851.00 90.00%=6933.00 95.00%=9145.00
      99.00%=59133.00 99.90%=208001.00 99.99%=505030.00
    [info] == put-many ==

## Many-clients

This test has one producer that trickles out one item at a time, and a pile of
consumers fighting for each item. It usually takes exactly as long as the
number of items times the delay, but is useful as a validation test to make
sure kestrel works as advertised without blowing up.

    $ sbt "many-clients --help"
    usage: many-clients [options]
        spin up N clients and have them do timeout reads on a queue while a
        single producer trickles out.

    options:
        -s MILLESCONDS
            sleep MILLISECONDS between puts (default: 100)
        -n ITEMS
            put ITEMS total items into the queue (default: 100)
        -c CLIENTS
            use CLIENTS consumers (default: 100)

A sample run on a 2010 MacBook Pro:

    [info] == many-clients ==
    [info] Running net.lag.kestrel.load.ManyClients
    Finished in 11104 msec.
    [info] == many-clients ==

## Flood

This test starts up one producer and one consumer, and just floods items
through kestrel as fast as it can.

    $ sbt "flood --help"
    usage: flood [options]
        spin up a producer and consumer and flood N items through kestrel

    options:
        -n ITEMS
            put ITEMS items into the queue (default: 10000)
        -k KILOBYTES
            put KILOBYTES per queue item (default: 1)

A sample run on a 2010 MacBook Pro:

    [info] == flood ==
    [info] Running net.lag.kestrel.load.Flood -n 100000
    flood: 100000 items of 1kB
    Finished in 16834 msec (168.3 usec/put throughput).
    Consumer spun 1 times in misses.

## Packing

This test starts up one producer and one consumer, seeds the queue with a
bunch of items to cause it to fall behind, then does cycles of flooding items
through the queue, separated by pauses. It's meant to test kestrel's behavior
with a queue that's fallen behind and *stays* behind indefinitely, to make
sure the journal files are packed periodically without affecting performance
too badly.

    $ sbt "packing --help"
    usage: packing [options]
        spin up a producer and consumer, write N items, then do read/write cycles
        with pauses

    options:
        -q NAME
            use named queue (default: spam)
        -n ITEMS
            put ITEMS items into the queue (default: 25000)
        -k KILOBYTES
            put KILOBYTES per queue item (default: 1)
        -t SECONDS
            pause SECONDS between cycles (default: 1)
        -c CYCLES
            do read/writes CYCLES times (default: 100)
        -x
            use transactions when fetching

A sample run on a 2010 MacBook Pro:

    [info] == packing ==
    [info] Running net.lag.kestrel.load.JournalPacking -c 10 -q small
    packing: 25000 items of 1kB with 1 second pauses
    Wrote 25000 items starting at 0.
    cycle: 1
    Wrote 25000 items starting at 25000.
    Read 25000 items in 5402 msec. Consumer spun 0 times in misses.
    cycle: 2
    Wrote 25000 items starting at 50000.
    Read 25000 items in 5395 msec. Consumer spun 0 times in misses.
    cycle: 3
    Wrote 25000 items starting at 75000.
    Read 25000 items in 4584 msec. Consumer spun 0 times in misses.
    cycle: 4
    Wrote 25000 items starting at 100000.
    Read 25000 items in 4455 msec. Consumer spun 0 times in misses.
    cycle: 5
    Wrote 25000 items starting at 125000.
    Read 25000 items in 4742 msec. Consumer spun 0 times in misses.
    cycle: 6
    Wrote 25000 items starting at 150000.
    Read 25000 items in 4468 msec. Consumer spun 0 times in misses.
    cycle: 7
    Wrote 25000 items starting at 175000.
    Read 25000 items in 5127 msec. Consumer spun 0 times in misses.
    cycle: 8
    Wrote 25000 items starting at 200000.
    Read 25000 items in 4357 msec. Consumer spun 0 times in misses.
    cycle: 9
    Wrote 25000 items starting at 225000.
    Read 25000 items in 4500 msec. Consumer spun 0 times in misses.
    cycle: 10
    Wrote 25000 items starting at 250000.
    Read 25000 items in 4558 msec. Consumer spun 0 times in misses.
    Read 25000 items in 3141 msec. Consumer spun 0 times in misses.
    [info] == packing ==

You can see the journals being packed in the kestrel log:

    INF [20110405-20:36:57.420] kestrel: Setting up queue small: maxItems=2147483647 maxSize=134217728.bytes maxItemSize=922
    3372036854775807.bytes maxAge=None defaultJournalSize=16777216.bytes maxMemorySize=16777216.bytes maxJournalSize=1342177
    28.bytes discardOldWhenFull=true keepJournal=true syncJournal=never expireToQueue=None maxExpireSweep=2147483647 fanoutO
    nly=false
    INF [20110405-20:36:57.421] kestrel: Replaying transaction journal for 'small'
    INF [20110405-20:36:57.422] kestrel: No transaction journal for 'small'; starting with empty queue.
    INF [20110405-20:36:57.422] kestrel: Finished transaction journal for 'small' (0 items, 0 bytes) xid=0
    INF [20110405-20:36:59.779] kestrel: Rotating journal file for 'small' (qsize=16440320)
    INF [20110405-20:36:59.852] kestrel: Dropping to read-behind for queue 'small' (16.0 MiB)
    INF [20110405-20:37:02.032] kestrel: Rotating journal file for 'small' (qsize=29139968)
    INF [20110405-20:37:04.583] kestrel: Rotating journal file for 'small' (qsize=35066880)
    INF [20110405-20:37:05.005] kestrel: Read-behind on 'small' moving from file small.1302061022051 to small.1302061024673
    INF [20110405-20:37:08.547] kestrel: Read-behind on 'small' moving from file small.1302061024673 to small
    INF [20110405-20:37:09.553] kestrel: Rotating journal file for 'small' (qsize=27975680)
    INF [20110405-20:37:12.412] kestrel: Read-behind on 'small' moving from file small.1302061029571 to small
    INF [20110405-20:37:14.511] kestrel: Rotating journal file for 'small' (qsize=26700800)
    INF [20110405-20:37:16.384] kestrel: Read-behind on 'small' moving from file small.1302061034588 to small
    INF [20110405-20:37:17.122] kestrel: Rotating journal file for 'small' (qsize=29371392)
    INF [20110405-20:37:20.164] kestrel: Read-behind on 'small' moving from file small.1302061037149 to small
    INF [20110405-20:37:21.410] kestrel: Rotating journal file for 'small' (qsize=26664960)
    INF [20110405-20:37:23.113] kestrel: Read-behind on 'small' moving from file small.1302061041427 to small
    INF [20110405-20:37:25.302] kestrel: Rotating journal file for 'small' (qsize=26168320)
    INF [20110405-20:37:27.118] kestrel: Read-behind on 'small' moving from file small.1302061045321 to small
    INF [20110405-20:37:27.119] kestrel: Rewriting journal file from checkpoint for 'small' (qsize=27889664)
    INF [20110405-20:37:27.129] kestrel: Packing journals for 'small': small.1302061019805, small.1302061022051, small.13020
    61024673, small.1302061029571, small.1302061034588, small.1302061037149, small.1302061041427, small.1302061045321
    INF [20110405-20:37:27.635] kestrel: Packing 'small' -- erasing old files.
    INF [20110405-20:37:27.646] kestrel: Packing 'small' done: small.1302061045321, small
    INF [20110405-20:37:28.115] kestrel: Rotating journal file for 'small' (qsize=28761088)
    INF [20110405-20:37:31.108] kestrel: Read-behind on 'small' moving from file small.1302061048143 to small
    INF [20110405-20:37:32.202] kestrel: Rotating journal file for 'small' (qsize=27242496)
    INF [20110405-20:37:34.048] kestrel: Read-behind on 'small' moving from file small.1302061052221 to small
    INF [20110405-20:37:36.255] kestrel: Rotating journal file for 'small' (qsize=25759744)
    INF [20110405-20:37:38.433] kestrel: Read-behind on 'small' moving from file small.1302061056360 to small
    INF [20110405-20:37:39.550] kestrel: Rotating journal file for 'small' (qsize=27325440)
    INF [20110405-20:37:42.266] kestrel: Read-behind on 'small' moving from file small.1302061059646 to small
    INF [20110405-20:37:43.464] kestrel: Rotating journal file for 'small' (qsize=26256384)
    INF [20110405-20:37:45.110] kestrel: Read-behind on 'small' moving from file small.1302061063469 to small
    INF [20110405-20:37:46.110] kestrel: Rotating journal file for 'small' (qsize=27487232)
    INF [20110405-20:37:48.928] kestrel: Read-behind on 'small' moving from file small.1302061066128 to small
    INF [20110405-20:37:49.875] kestrel: Rotating journal file for 'small' (qsize=28101632)
    INF [20110405-20:37:51.801] kestrel: Read-behind on 'small' moving from file small.1302061069893 to small
    INF [20110405-20:37:51.801] kestrel: Rewriting journal file from checkpoint for 'small' (qsize=26379264)
    INF [20110405-20:37:51.804] kestrel: Packing journals for 'small': small.1302061045321, small.1302061048143, small.13020
    61052221, small.1302061056360, small.1302061059646, small.1302061063469, small.1302061066128, small.1302061069893
    INF [20110405-20:37:52.237] kestrel: Packing 'small' -- erasing old files.
    INF [20110405-20:37:52.246] kestrel: Packing 'small' done: small.1302061069893, small
    INF [20110405-20:37:54.012] kestrel: Rotating journal file for 'small' (qsize=26510336)
    INF [20110405-20:37:55.808] kestrel: Read-behind on 'small' moving from file small.1302061074039 to small
    INF [20110405-20:37:56.594] kestrel: Rotating journal file for 'small' (qsize=29006848)
    INF [20110405-20:37:59.363] kestrel: Read-behind on 'small' moving from file small.1302061076614 to small
    INF [20110405-20:37:59.731] kestrel: Coming out of read-behind for queue 'small'

