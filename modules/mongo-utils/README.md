# Wordnik MongoDB Utils

## Overview
This project contains utils for working with MongoDB

## To Build
Build the project with maven:

<pre>
mvn package
</pre>

Or get the artifacts from maven central

### Utilities Included
#### com.wordnik.mongo.connection.MongoDBConnectionManager

A wrapper around the MongoDB java driver which allows the following:

<li>- Friendly, string-based named connection management:</li>

<li>- Auto-detection of master/slave or replica-sets</li>

<li>- Simple connection pooling, isolated to a single host:port/schema</li>

See the [tests](https://github.com/wordnik/wordnik-oss/blob/master/modules/mongo-utils/src/test/scala/com/wordnik/test/mongo/connection/ConnectionManagerTest.scala) for examples

Note!  The tests require a mongodb master running on port 27017 and a replica-set on ports 27018-27020.
The integration-test will be updated with a mongodb-maven plugin shortly to support this.