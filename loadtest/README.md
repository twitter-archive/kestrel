Parrot Load Test Harness
========================

Runnning an existing test
-------------------------

From the project root directory...

    $ cd loadtest
    $ sbt update package-dist  # only necessary if libs change
    $ sbt start-parrot-consumer
    ...
    $ sbt start-parrot-producer
    ...

Modify the config files in loadtest/config to choose victims,
duration, etc.
