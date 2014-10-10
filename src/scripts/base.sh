#!/bin/bash

# Maven, sbt, and pants builds result in different jar names. This is a base script class that other scripts
# can inherit to find the appropriate main and test jars.

set -e

SBT_JAR=@DIST_NAME@-@VERSION@.jar
MVN_JAR=kestrel-@VERSION@.jar
PANTS_JAR=kestrel.jar
SBT_TEST_JAR=@DIST_NAME@-@VERSION@-test.jar
MVN_TEST_JAR=kestrel-tests.jar

# Finds main jar given jar path, otherwise, dies.
function find_jar() {
  jar_path=$1
  if [ -e "$jar_path/$MVN_JAR" ]; then
    jar=$jar_path/$MVN_JAR
  elif [ -e "$jar_path/$SBT_JAR" ]; then
    jar=$jar_path/$SBT_JAR
  elif [ -e "$jar_path/$PANTS_JAR" ]; then
    jar=$jar_path/$PANTS_JAR
  else
    echo "Unable to find jar in $(cd $jar_path && pwd)" >&2
    exit 1
  fi
  echo $jar
}

# Finds test jar given jar path, otherwise, dies.
function find_test_jar() {
  jar_path=$1
  if [ -e "$jar_path/$MVN_TEST_JAR" ]; then
    test_jar=$jar_path/$MVN_TEST_JAR
  elif [ -e "$jar_path/$SBT_TEST_JAR" ]; then
    test_jar=$jar_path/$SBT_TEST_JAR
  else
    echo "Unable to find test jar in $(cd $jar_path && pwd)" >&2
    exit 1
  fi
  echo $test_jar
}

