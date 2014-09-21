cassandra-top
=============

Cassandra top command to monitor cluster named cass_top, similar to the linux top command.

cass_top is a wrapper around the Cassandra nodetool command that:

- colorizes the nodes by status and updates every few seconds
- has interactive option keys for most nodetool reporting options, documented on the help screen
- organizes nodetool commands by groups (cluster-wide, node-specific, reporting-only and updating)
- builds the nodetool command for you and displays it for confirmation before running
- populates menus of nodes, keyspaces and column families
- bash only (no dependencies except nodetool)
- no additional firewall holes needed
- minimal output to show multiple Cassandra rings on the same monitor.

usage: cass_top [connection_host=localhost] [keyspace=all] [one character option=none]

![cass_top screenshot](cass_top.png?raw=true "cass_top screenshot")
![cass_top_help screenshot](cass_top_help.png?raw=true "cass_top_help screenshot")

