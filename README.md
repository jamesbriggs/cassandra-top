cassandra-top
=============

Cassandra top command to monitor cluster cass_top

cass_top is a wrapper around the Cassandra nodetool command that colorizes the nodes by status.

- bash only (no dependencies except nodetool)
- no firewall holes needed
- minimal output to show multiple Cassandra rings on the same monitor

Usage: cass_top [host] [keyspace]

![cass_top screenshot](cass_top.png?raw=true "cass_top screenshot")

