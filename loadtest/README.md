Parrot Load Test Harness
========================

Runnning an existing test
-------------------------

From the project root directory...

    $ cd loadtest
    $ sbt update package-dist  # only necessary if libs change
    $ sbt run-parrot\ config/kestrel-memcache-consumer.scala
    ...
    $ sbt run-parrot\ config/kestrel-memcache-producer.scala
    ...

Modify the config files in loadtest/config to choose victims,
duration, etc.
