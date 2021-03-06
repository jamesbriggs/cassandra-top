cassandra-top
=============

**Summary**

The Cassandra top command, cass_top, is a friendly UI for monitoring clusters,
similar to the top command, and shows menus to build and run nodetool commands.

**Features**

cass_top is a wrapper around the Cassandra nodetool command that:

- colorizes the nodes by status and updates every few seconds
- has interactive option keys for all nodetool commands and options, documented on the help screen
- organizes nodetool commands by groups (cluster-wide, node-specific, reporting-only and updating)
- populates menus of nodes, keyspaces and column families
- builds the nodetool command and displays it for confirmation before running
- logs nodetool command start and end time, userid, stdout, stderr and the exit code.

**Technical Details**

- bash only (no dependencies except nodetool), tested on linux and Mac OS X
- no additional firewall holes needed beyond what nodetool requires
- minimal output to show multiple Cassandra rings on the same monitor.

`usage: cass_top [connection_host=localhost] [keyspace=all] [one-character option=none]`

**For Contributors**

To run the provided tests with Cassandra nodetool:

- `HOST=1.2.3.4 make test`

To run the provided tests with mock nodetool (useful for testing without a network, on your notebook or for porting):

- `MOCK=1 HOST=1.2.3.4 make test`

To run cass_top under the bashdb debugger:

- install bashdb and add it to your PATH
- do 'ps' to see your shell's pty number (NN) then:
- `echo "source /dev/pts/NN" > ~/.bashdbinit`
- `MOCK=1 HOST=1.2.3.4 bashdb cass_top` then type 'c' ENTER. There are scripted breakpoints to get you started debugging.

![cass_top screenshot](cass_top.png?raw=true "cass_top screenshot")
![cass_top_help screenshot](cass_top_help.png?raw=true "cass_top_help screenshot")

