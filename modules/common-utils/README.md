# Wordnik Common Utils

## Overview
This project contains a number of tools used by Wordnik

## To Build
Build the project with maven:

<pre>
mvn package
</pre>

Or get the artifacts from maven central

### Utilities Included
#### com.wordnik.util.perf.Profile

A utility for fine-grained code instrumentation.  Simply pass a function to the Profile class with a name
and the Profiler will keep track of performance statistics.  For example:

<pre>
Profile("delete user", {
  // do something
})
</pre>

This keeps track of count, min, max, avg, total durations to execute the block of code.  You can pull the
perf data as follows:

<pre>
Profile.getCounters("delete user")
</pre>

Or use a screen printer to write them to stdout:

<pre>
ProfileScreenPrinter.dump
</pre>

You can also set a trigger to fire on closure of a Profile operation.  This is handy as a trigger for
long-running operations, APM, etc.  See [here](https://github.com/wordnik/wordnik-oss/blob/master/modules/common-utils/src/test/scala/com/wordnik/test/util/perf/ProfileTriggerTest.scala#L16).