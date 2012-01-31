# Wordnik open-source components

## Overview
This project contains a number of tools for common routines, mongodb usage & administration, and swagger

### To build
Requires maven 2 or higher, Java 1.6.X.  You can also find the artifacts in [maven central](http://repo1.maven.org/maven2/com/wordnik/)

First build:
<pre>
mvn -N
</pre>

Subsequent builds:
<pre>
mvn package
</pre>

This will build all the OSS modules.  You can see more on each modules home page:

[common-utils](/wordnik/wordnik-oss/blob/master/modules/common-utils/README.md) Common convienence utilities

[mongo-utils](/wordnik/wordnik-oss/blob/master/modules/mongo-utils/README.md) Core MongoDB utilities

[mongo-admin-utils](/wordnik/wordnik-oss/blob/master/modules/mongo-admin-utils/README.md) Set of tools to maintain a production MongoDB deployment

[swagger-jaxrs-utils](/wordnik/wordnik-oss/blob/master/modules/swagger-jaxrs-utils/README.md) Add-ons for Swagger with JAX-RS
