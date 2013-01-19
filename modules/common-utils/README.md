# Wordnik Common Utils

## Overview
This project contains a number of tools used by Wordnik

## To Build
Build the project with maven:

```bash
cd ../..
./sbt compile
```

Or get the artifacts from maven central

### Utilities Included
#### com.wordnik.util.perf.Profile

A utility for fine-grained code instrumentation.  Simply pass a function to the Profile class with a name
and the Profiler will keep track of performance statistics. It registers timings and has support for akka's futures
in 2.9.x or uses scala.concurrent.Future in 2.10.x

For example:

```scala
// For application level metrics
Profile.global("delete user")) {
  // do something
}

// or for instance level metrics
val profiler = Profile[UserDao]
profiler("delete user") {
  // do something
}

```

This utility is backed by coda hale metrics. For reporting etc you can use jconsole or one of the reporters.
[coda hale metrics](http://metrics.codahale.com/manual/core/#man-core-reporters-console)