# wordnik open-source tools

## Overview
These are tools used to maintain a MongoDB deployment.

### To build
Requires apache ant 1.7 or greater, java 6:

<pre>
cd src/java
ant install.ivy
ant dist
</pre>

To run tools:
<pre>
cd dist (or wherever you unzipped the distribution)
./bin/run.sh <tool-class> <options>
</pre>

### To get toop options
Run any tool with a -? parameter to see the options:

<pre>./bin/run.sh com.wordnik.system.mongodb.SnapshotUtil -?
usage: SnapshotUtil
 -c : CSV collection string (prefix with ! to exclude)
 -d : database name
 -h : hostname
 -t : threads
 -o : output directory
 [-s : max file size in MB]
 [-Z : compress files]
 [-J : output in JSON (default is BSON)]
 [-u : username]
 [-p : password]
</pre>

### Tools included:
<pre>com.wordnik.system.mongodb.SnapshotUtil</pre>
This is pretty straight forward, it's meant for taking backups of your data.  The differences between it and mongodump are:
<li>It splits files based on a configurable size</li>
<li>It will let you select what you want to backup with inclusion and exclusion operators</li>
<li>It will automatically gzip the files as it rotates them</li>
<li>It supports a JSON export</li>
<li>It runs a configurable thread pool so you can backup multiple collections simultaneously</li>


<pre>com.wordnik.system.mongodb.RestoreUtil</pre>

Operates against either mongodump files or files made with the SnapshotUtil with either uncompressed or compressed bson files. Also supports inclusion/exclusion of files

<pre>com.wordnik.system.mongodb.IncrementalBackupUtil</pre>

This queries a master server's oplog and maintains a set of files which can be replayed against a snapshot of the database.  The procedure we use is to snapshot the db (either at the filesystem or with the tool) and apply the incremental changes created by this tool.
