cassandra-top
=============

Summary

Cassandra top command named cass_top is a user-friendly UI for monitoring clusters,
similar to the linux top command, and shows menus to build and run nodetool commands.

Features

cass_top is a wrapper around the Cassandra nodetool command that:

- colorizes the nodes by status and updates every few seconds
- has interactive option keys for all nodetool commands and options, documented on the help screen
- organizes nodetool commands by groups (cluster-wide, node-specific, reporting-only and updating)
- populates menus of nodes, keyspaces and column families
- builds the nodetool command and displays it for confirmation before running
- logs start and end time, userid, nodetool stdout, stderr and exit code.

Technical Details

- bash only (no dependencies except nodetool)
- no additional firewall holes needed beyond what nodetool requires
- minimal output to show multiple Cassandra rings on the same monitor.

usage: cass_top [connection_host=localhost] [keyspace=all] [one-character option=none]

To run the provided tests:

- HOST=1.2.3.4 make test

![cass_top screenshot](cass_top.png?raw=true "cass_top screenshot")
![cass_top_help screenshot](cass_top_help.png?raw=true "cass_top_help screenshot")

