
Folder layout:
  src/  -- source code
    main/
      scala/
      java/
      jni/
      thrift/
      resources/  -- files to copy into dist
    test/
      scala/
      resources/  -- files needed only for tests
    scripts/
  ivy/
    ivy.xml  -- package description
    ivysettings.xml  -- repositories
  config/  -- items to package in the distribution but not in jars

Created during the build:
  target/  -- compiled files
    classes/
    test-classes/
      resources/  -- copied from src/test/resources/
    gen-java/  -- generated from thrift
    scripts/
  dist/
    <package>-<version>/
      <package>-<version>.jar
      *.so  -- if jni stuff was compiled
      *.jnilib  -- if jni stuff was compiled
      libs/  -- dependent jars (and any extra)
      config/  -- copied over from config/
      resources/  -- from src/main/resources/


... tbd ...

Primary targets
---------------
  - clean
    - erase all generated/built files
  - distclean
    - clean *everything*
  - prepare
    - resolve dependencies and download them
  - compile
    - build any java/scala source
  - test
    - build and run test suite (requires e:testclass)
  - stress
    - (optional) run stress test suite (requires e:stresstestclass)
  - docs
    - generate documentation
  - package
    - copy all generated/built files into distribution folder
      (requires "local" repo)


Properties that can change behavior
-----------------------------------

- skip.download
    don't download ivy; assume it's present
- skip.test
    don't run test suite
- skip.docs
    don't build docs
- libs.extra
    any extra files to copy into dist/<p>/libs/ during compile
- dist.extra
    any extra files to copy into dist/<p> during compile
- config.extra
    any extra files to copy into config/ during compile
- pack.deps
    pack dependent jars into the final dist jar, to remove dependencies


Extra ivy thingies
------------------

- e:buildpackage
    causes a build.properties file to be created in the named package
    (stores version #, etc -- used by RuntimeEnvironment in configgy)
- e:thriftpackage
    output package for generated thrift classes; causes thrift DDLs to be
    compiled
- e:testclass
    class to execute for unit tests -- required, in order to run tests
- e:stresstestclass
    class to execute for stress tests (optional)
- e:jarclassname
    creates an executable jar with this as the main class name


JNI
---

JNI will be built if there appears to be a `build.xml` file in src/main/jni/.
That ant file should contain a "clean" target and a "compile" target.

Post-compile, the jni/ folder is expected to look like this:
  src/
    main/
      jni/
        build.xml  -- used to build the jni packages
        <package>/
          target/
            *.so  -- copied into dist/<p>/
            *.jnilib  -- copied into dist/<p>/
            *.jar  -- copied into dist/<p>/

There may be as many <package> folders in jni/ as you desire.

