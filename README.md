cassandra-top
=============

Cassandra top command to monitor cluster named cass_top, similar to the linux top command.

cass_top is a wrapper around the Cassandra nodetool command that:

- colorizes the nodes by status
- has interactive option keys for several nodetool reporting options, documented on the help screen
- bash only (no dependencies except nodetool and grep)
- no additional firewall holes needed
- minimal output to show multiple Cassandra rings on the same monitor.

Usage: cass_top [host] [keyspace]

![cass_top screenshot](cass_top.png?raw=true "cass_top screenshot")
![cass_top_help screenshot](cass_top_help.png?raw=true "cass_top_help screenshot")

