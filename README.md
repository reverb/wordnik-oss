# wordnik open-source tools

## Overview
These are tools used to maintain a MongoDB deployment.

### To build
Requires apache ant 1.7 or greater, java 6:

<pre>
cd src/java
ant -f install-ivy # only needed if you don't have apache ivy installed
ant dist
</pre>

### To run
<pre>
cd dist (or wherever you unzipped the distribution)
./bin/run.sh <tool-class> <options>
</pre>


### To get tool options
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


### Tools included
####com.wordnik.system.mongodb.SnapshotUtil
This is pretty straight forward, it's meant for taking backups of your data.  Some differences between it and mongodump are:

<li>It splits files based on a configurable size</li>

<li>It will let you select what you want to backup with inclusion and exclusion operators</li>

<li>It will automatically gzip the files as it rotates them</li>

<li>It supports a JSON export</li>

<li>It runs a configurable thread pool so you can backup multiple collections simultaneously</li>

examples:
backup localhost/test to folder backup
<pre>./bin/run.sh com.wordnik.system.mongodb.SnapshotUtil -h localhost -d test -o backup</pre>

backup only collection "users" in localhost/test to folder "backups"
<pre>./bin/run.sh com.wordnik.system.mongodb.SnapshotUtil -h localhost -d test -c users -o backups</pre>

backup collection "user_links" in localhost/test to folder backups in JSON format in 32mb files, then gzip
<pre>./bin/run.sh com.wordnik.system.mongodb.SnapshotUtil -h localhost -d test -c user_links -o backups -J -s 32 -Z</pre>

#### com.wordnik.system.mongodb.RestoreUtil
Operates against either mongodump files or files made with the SnapshotUtil with either uncompressed or compressed bson files. Also supports inclusion/exclusion of files

examples:
Restore all files in folder "backups" to database "restored_data"
<pre>./bin/run.sh com.wordnik.system.mongodb.RestoreUtil -i backup -h localhost -d restored_data</pre>

#### com.wordnik.system.mongodb.IncrementalBackupUtil
This queries a master server's oplog and maintains a set of files which can be replayed against a snapshot of the database.  The procedure we use is to snapshot the db (either at the filesystem or with the tool) and apply the incremental changes created by this tool.

The tool looks for a file called "last_timestamp.txt" in the output directory.  This file sets a starting point for querying the oplog--it should contain a single line in the format:

<pre>[time-in-seconds]|[counter]</pre>

The [time-in-seconds] is the seconds since epoch, which you can grab from the OS or from a tool like this:

http://www.esqsoft.com/javascript_examples/date-to-epoch.htm

The counter should typically be set to 0.  As the tool runs, every operation flushed to disk will cause this file to be updated.  If you want to stop a running process, create a file called "stop.txt" in the CWD of the application.  It will cause the app to stop within one second.

examples:
Save incremental backup on everything in the database "ugc" (note the -c args are scoped to the database)
<pre>./bin/run.sh com.wordnik.system.mongodb.IncrementalBackupUtil -c ugc -o backups</pre>

Save incremental backup on the database "ugc" with collection "login_info" to the folder "backups"
<pre>./bin/run.sh com.wordnik.system.mongodb.IncrementalBackupUtil -c ugc.login_info -o backups</pre>

#### com.wordnik.system.mongodb.ReplayUtil

Takes a series of files created by the IncrementalBackupUtil and replays them.  The tool allows applying the operations against alternate databases and collections.  It also supports skipping records which fall outside a specified timepoint, if for instance you want to roll back to a particular point in time.

Note that not all operations can be remapped, especially adding indexes and applying database-level commands.

examples:
Replay incremental backup files in folder "backups" to host "localhost:27018"
<pre>./bin/run.sh com.wordnik.system.mongodb.ReplayUtil -i backups -h localhost:27018</pre>

Replay files in folder "backups" to host "localhost:27018", collection "test1", and map from db "test" to db "foo".  Note!  This doesn't replay commands applied to remapped databases
<pre>./bin/run.sh com.wordnik.system.mongodb.ReplayUtil -i backups  -h localhost:27018 -R test=foo -c test1</pre>

Replay files in folder backups to host localhost:27018, collection test1, and map from test1 to test5
<pre>./bin/run.sh com.wordnik.system.mongodb.ReplayUtil -i backups  -h localhost:27018 -r test1=test5 -c test1</pre>

#### com.wordnik.system.mongodb.ReplicationUtil
Tool to replicate from server to server.  Use caution with the config to avoid getting in a replication loop!

examples:
Replicate everything from localhost:27017 to localhost:27018
<pre>./bin/run.sh com.wordnik.system.mongodb.ReplicationUtil -h localhost -H localhost:27018</pre>

Replicate collection test.foo from localhost:27017 to localhost:27018, including commands, index operations
<pre>./bin/run.sh com.wordnik.system.mongodb.ReplicationUtil -h localhost -H localhost:27018 -c test.foo,test.$cmd,test.system.indexes</pre>
