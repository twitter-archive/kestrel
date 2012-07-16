<img src="kestrel-hovering.png" style="float: right"></a>

Kestrel
=======

Kestrel is a simple, distributed message queue written on the JVM, based on
Blaine Cook's "starling".

Each server handles a set of reliable, ordered message queues, with no cross
communication, resulting in a cluster of k-ordered ("loosely ordered") queues.
Kestrel is fast, small, and reliable.

Features
--------

- memcache protocol
- thrift protocol
- journaled (durable) queues
- fanout queues (one writer, many readers)
- item expiration
- transactional reads

For more information, check out the [readme](readme.html).

News
----

- **16 Jul 2012** - *kestrel 2.3.1 released*

- **11 May 2012** - *kestrel 2.2.0 released*

- **12 Jan 2012** - *kestrel 2.1.5 released*

- **21 Nov 2011** - *kestrel 2.1.4 released*

- **13 Oct 2011** - *kestrel 2.1.3 released*

- **29 Sep 2011** - *kestrel 2.1.2 released*

- **23 Sep 2011** - *kestrel 2.1.1 released*

- **27 Aug 2011** - *kestrel 2.1 released*

Further Reading
---------------

- [A working guide to kestrel](docs/guide.html)

- [Wiki pages](https://github.com/robey/kestrel/wiki) on github

- [Slides from a tech talk](kestrel-tech-talk-jun2011.pdf) on the state of kestrel 2.0, in June 2011

- [Scaladocs](http://robey.github.com/kestrel/api/main/api/index.html)

Downloads
---------

- [kestrel 2.3.1](download/kestrel-2.3.1.zip)

- [kestrel 2.2.0](download/kestrel-2.2.0.zip)

- [kestrel 2.1.5](download/kestrel-2.1.5.zip)

- [kestrel 2.1.4](download/kestrel-2.1.4.zip)

- [kestrel 1.2.2.1](download/kestrel-1.2.2.1.zip)

- [older releases](download/)

Development
-----------

Development of kestrel occurs on github:

- [http://github.com/robey/kestrel](http://github.com/robey/kestrel)

Join our mailing list:

- [kestrel-talk@googlegroups.com](http://groups.google.com/group/kestrel-talk)


<div style="font-size: 75%; margin-left: 0px; font-style: italic">
(photo courtesy of the
<a href="http://www.rspb.org.uk/wildlife/birdguide/name/k/kestrel/index.aspx">Royal Society for the Protection of Birds</a>)
</div>
