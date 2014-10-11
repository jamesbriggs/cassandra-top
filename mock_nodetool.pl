#!/usr/bin/perl

# Program: mock_nodetool.pl
# Purpose: emulate the reporting features of Cassandra nodetool so that a cluster is not needed for most UI testing (ie. this is a mock or fake object)
# License: Copyright 2014 by James Briggs, San Jose, California, USA. Released under Apache 2 software license
# Github: https://github.com/jamesbriggs/cassandra-top/blob/master/cass_top
# Date: 2014 09 23
# Env: Perl5
# Notes:
# - the original nodetool CLI options actually uses most of the features of the Java Apache Commons CLI library parser,
#   which is implemented here with custom @ARGV pre-processing and GetOpt::Long in process_args()
# - there is a mock network device called "bad" if you need to generate the nodetool 'cannot resolve' error message

use strict;
use warnings;

use Data::Dumper;

use Getopt::Long qw(:config bundling_override pass_through no_auto_abbrev no_ignore_case prefix_pattern=--|-);

   my $DEBUG = 1;

   my %opts;
   my $go_err=0;

#  original nodetool output strings ...

   my $str_version = "ReleaseVersion: 2.0.9\n";

   my $str_getstreamthroughput = "Current stream throughput: 200 MB/s\n";

   my $str_getcompactionthroughput = "Current compaction throughput: 200 MB/s\n";

   my $str_compactionstats = <<EOF;
pending tasks: 0
Active compaction remaining time :        n/a"
EOF

   my $str_status = <<EOF;
Note: Ownership information does not include topology; for complete information, specify a keyspace
Datacenter: DC1
===============
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens  Owns   Host ID                               Rack
UN  10.0.1.130  77.86 MB   256     13.7%  9ec944e1-0abf-4334-a32f-c0b947196c77  RAC1
UN  10.0.1.133  82.71 MB   256     11.8%  8302c6d5-4c88-4695-bbf4-762bc7f24544  RAC2
UN  10.0.1.134  93.45 MB   256     12.2%  eddb03b2-98d3-46ff-be63-95435414a883  RAC2
UN  10.0.1.136  68.26 MB   256     12.9%  92d522a3-de98-45e0-b0ab-0baa4a6972dd  RAC2
UN  10.0.1.137  65.07 MB   256     11.9%  ba667629-d1ee-4c2a-9ea5-59065272f0dc  RAC2
UN  10.0.1.138  64.65 MB   256     12.0%  c35a2261-a65f-4980-97a2-c4f82e034d4b  RAC1
UN  10.0.1.140  86.57 MB   256     12.8%  ceb2924f-2e8b-41e9-8af8-2890c7e41ba5  RAC1
DN  10.0.1.141  56.92 MB   256     12.7%  2a8dde5e-29b0-4a67-8204-40769376c44a  RAC1
EOF

   my $str_help = <<EOF;
usage: java org.apache.cassandra.tools.NodeCmd --host <arg> <command>
            
 -a,--include-all-sstables   includes sstables that are already on the
                             most recent version during upgradesstables
 -c,--compact                print histograms in a more compact format
 -cf,--column-family <arg>   only take a snapshot of the specified table
                             (column family)
 -dc,--in-dc <arg>           only repair against nodes in the specified
                             datacenters (comma separated)
 -et,--end-token <arg>       token at which repair range ends
 -h,--host <arg>             node hostname or ip address
 -hosts,--in-host <arg>      only repair against specified nodes (comma
                             separated)
 -i,--ignore                 ignore the supplied list of
                             keyspace.columnfamiles in statistics
 -local,--in-local-dc        only repair against nodes in the same
                             datacenter
 -ns,--no-snapshot           disables snapshot creation for scrub
 -p,--port <arg>             remote jmx agent port number
 -par,--parallel             repair nodes in parallel.
 -pr,--partitioner-range     only repair the first range returned by the
                             partitioner for the node
 -pw,--password <arg>        remote jmx agent password
 -r,--resolve-ip             show node domain names instead of IPs
 -s,--skip-corrupted         when scrubbing counter tables, skip corrupted
                             rows
 -st,--start-token <arg>     token at which repair range starts
 -T,--tokens                 display all tokens
 -t,--tag <arg>              optional name to give a snapshot
 -u,--username <arg>         remote jmx agent username

Available commands
  cfhistograms <keyspace> <cfname> - Print statistic histograms for a given column family
  cfstats [keyspace].[cfname] ... - Print statistics on column families. Use the -i flag to ignore the list of column families and display the remaining cfs.
  cleanup [keyspace] [cfnames] - Run cleanup on one or more column families
  clearsnapshot [keyspaces...] -t [snapshotName] - Remove snapshots for the specified keyspaces. Either remove all snapshots or remove the snapshots with the given name.
  compact [keyspace] [cfnames] - Force a (major) compaction on one or more column families
  compactionhistory      - Print history of compaction
  compactionstats        - Print statistics on compactions
  decommission           - Decommission the *node I am connecting to*
  describecluster        - Print the name, snitch, partitioner and schema version of a cluster.
  describering [keyspace] - Shows the token ranges info of a given keyspace.
  disableautocompaction [keyspace] [cfnames] - Disable autocompaction for the given keyspace and column family
  disablebackup          - Disable incremental backup
  disablebinary          - Disable native transport (binary protocol)
  disablegossip          - Disable gossip (effectively marking the node down)
  disablehandoff         - Disable the future hints storing on the current node
  disablethrift          - Disable thrift server
  drain                  - Drain the node (stop accepting writes and flush all column families)
  enableautocompaction [keyspace] [cfnames] - Enable autocompaction
  enablebackup           - Enable incremental backup
  enablebinary           - Reenable native transport (binary protocol)
  enablegossip           - Reenable gossip
  enablehandoff          - Reenable the future hints storing on the current node
  enablethrift           - Reenable thrift server
  flush [keyspace] [cfnames] - Flush one or more column families
  getLoggingLevels       - Get the runtime logging levels
  getcompactionthreshold <keyspace> <cfname> - Print min and max compaction thresholds for a given column family
  getcompactionthroughput - Print the MB/s throughput cap for compaction in the system
  getendpoints <keyspace> <cf> <key> - Print the end points that owns the key
  getsstables <keyspace> <cf> <key> - Print the sstable filenames that own the key
  getstreamthroughput    - Print the MB/s throughput cap for streaming in the system
  gossipinfo             - Shows the gossip information for the cluster
  info [-T/--tokens]     - Print node information (uptime, load, ...)
  invalidatekeycache     - Invalidate the key cache
  invalidaterowcache     - Invalidate the row cache
  join                   - Join the ring
  move <new token>       - Move node on the token ring to a new token. (for negative tokens, use \\ to escape, Example: move \\-123)
  netstats [host]        - Print network information on provided host (connecting node by default)
  pausehandoff           - Pause hints delivery process
  proxyhistograms        - Print statistic histograms for network operations
  rangekeysample         - Shows the sampled keys held across all keyspaces.
  rebuild [src-dc-name] - Rebuild data by streaming from other nodes (similarly to bootstrap)
  rebuild_index <keyspace> <cf-name> <idx1,idx1> - a full rebuild of native secondary indexes for a given column family. IndexNameExample: Standard3.IdxName,Standard3.IdxName1
  refresh <keyspace> <cf-name> - Load newly placed SSTables to the system without restart.
  reloadtriggers         - reload trigger classes
  removenode status|force|<ID> - Show status of current node removal, force completion of pending removal or remove provided ID
  repair [keyspace] [cfnames] - Repair one or more column families
   Use -dc to repair specific datacenters (csv list).
   Use -et to specify a token at which repair range ends.
   Use -local to only repair against nodes in the same datacenter.
   Use -pr to repair only the first range returned by the partitioner.
   Use -par to carry out a parallel repair.
   Use -st to specify a token at which the repair range starts.
  resetlocalschema       - Reset node's local schema and resync
  resumehandoff          - Resume hints delivery process
  ring                   - Print information about the token ring
  scrub [keyspace] [cfnames] [-s|--skip-corrupted] - Scrub (rebuild sstables for) one or more column families.
   Use -s/--skip-corrupted to skip corrupted rows even when scrubbing
   tables that use counters.
  setLoggingLevel        - Set a log level for a given logger. If both classQualifer and level are empty/null, it will reset to the initial configuration
  setcachecapacity <key-cache-capacity> <row-cache-capacity> - Set global key and row cache capacities (in MB units).
  setcachekeystosave <key-cache-keys-to-save> <row-cache-keys-to-save> - Set number of keys saved by each cache for faster post-restart warmup. 0 to disable.
  setcompactionthreshold <keyspace> <cfname> - Set min and max compaction thresholds for a given column family
  setcompactionthroughput <value_in_mb> - Set the MB/s throughput cap for compaction in the system, or 0 to disable throttling.
  setstreamthroughput  <value_in_mb> - Set the MB/s throughput cap for streaming in the system, or 0 to disable throttling.
  settraceprobability [value] - Sets the probability for tracing any given request to value. 0 disables, 1 enables for all requests, 0 is the default
  snapshot [keyspaces...] -cf [columnfamilyName] -t [snapshotName] - Take a snapshot of the optionally specified column family of the specified keyspaces  using optional name snapshotName
  status                 - Print cluster information (state, load, IDs, ...)
  statusbinary           - Status of native transport (binary protocol)
  statusthrift           - Status of thrift server
  stop <compaction_type> - Supported types are COMPACTION, VALIDATION, CLEANUP, SCRUB, INDEX_BUILD
  taketoken <token, ...> - Move the token(s) from the existing owner(s) to this node.  For vnodes only.  Use \\ to escape negative tokens.
  tpstats                - Print usage statistics of thread pools
  truncatehints <host-name> - Truncate all hints on the local node, or truncate hints for the endpoint specified.
  upgradesstables [-a|--include-all-sstables] [keyspace] [cfnames] - Rewrite sstables (for the requested column families) that are not on the current version (thus upgrading them to said current version).
   Use -a to include all sstables, even those already on the current version.
  version                - Print cassandra version
EOF

   my $str_info = <<EOF;
Token            : (invoke with -T/--tokens to see all 256 tokens)
ID               : ceb2924f-2e8b-41e9-8af8-2890c7e41ba5
Gossip active    : true
Thrift active    : true
Native Transport active: true
Load             : 92.62 MB
Generation No    : 1411067227
Uptime (seconds) : 557663
Heap Memory (MB) : 804.41 / 1956.00
Data Center      : DC1
Rack             : RAC1
Exceptions       : 0
Key Cache        : size 16552 (bytes), capacity 101711872 (bytes), 5288 hits, 5328 requests, NaN recent hit rate, 14400 save period in seconds
Row Cache        : size 0 (bytes), capacity 0 (bytes), 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds"
EOF

   my $str_info_T = <<EOF;
ID               : ceb2924f-2e8b-41e9-8af8-2890c7e41ba5
Gossip active    : true
Thrift active    : true
Native Transport active: true
Load             : 92.62 MB
Generation No    : 1411067227
Uptime (seconds) : 557884
Heap Memory (MB) : 867.18 / 1956.00
Data Center      : DC1
Rack             : RAC1
Exceptions       : 0
Key Cache        : size 16552 (bytes), capacity 101711872 (bytes), 5288 hits, 5328 requests, NaN recent hit rate, 14400 save period in seconds
Row Cache        : size 0 (bytes), capacity 0 (bytes), 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
Token            : -9159578207340468218
Token            : -9090960695561522598
Token            : -9077115458327522696
Token            : -8944258820915401024
Token            : -8927962076912678670
Token            : -8791606420276619399
Token            : -8766627947665412732
EOF

   my $str_gossipinfo = <<EOF;
/10.0.1.136
  generation:1410906006
  heartbeat:2160361
  STATUS:NORMAL,-1022224348962756527
  HOST_ID:92d522a3-de98-45e0-b0ab-0baa4a6972dd
  RELEASE_VERSION:2.0.9
  LOAD:7.1601653E7
  RPC_ADDRESS:10.0.1.136
  SCHEMA:d9e0f9ae-6221-3987-81fb-1a0134e02b22
  SEVERITY:0.0
  DC:DC1
  NET_VERSION:7
  RACK:RAC2
/10.0.1.137
  generation:1410898647
  heartbeat:2182667
  STATUS:NORMAL,-1062262977753289953
  HOST_ID:ba667629-d1ee-4c2a-9ea5-59065272f0dc
  RELEASE_VERSION:2.0.9
  LOAD:6.8247088E7
  RPC_ADDRESS:10.0.1.137
  SCHEMA:d9e0f9ae-6221-3987-81fb-1a0134e02b22
  SEVERITY:0.0
  DC:DC1
  NET_VERSION:7
  RACK:RAC2
/10.0.1.130
  generation:1411067279
  heartbeat:1671345
  STATUS:NORMAL,-106692201839339278
  HOST_ID:9ec944e1-0abf-4334-a32f-c0b947196c77
  RELEASE_VERSION:2.0.9
  LOAD:8.838359E7
  RPC_ADDRESS:10.0.1.130
  SCHEMA:d9e0f9ae-6221-3987-81fb-1a0134e02b22
  SEVERITY:0.0
  DC:DC1
  NET_VERSION:7
  RACK:RAC1
/10.0.1.140
  generation:1411067227
  heartbeat:1671482
  STATUS:NORMAL,-1030898123156584416
  HOST_ID:ceb2924f-2e8b-41e9-8af8-2890c7e41ba5
  RELEASE_VERSION:2.0.9
  LOAD:9.7127565E7
  RPC_ADDRESS:127.0.0.1
  SCHEMA:d9e0f9ae-6221-3987-81fb-1a0134e02b22
  SEVERITY:0.0
  NET_VERSION:7
  DC:DC1
  RACK:RAC1
/10.0.1.133
  generation:1410906095
  heartbeat:2160091
  STATUS:NORMAL,-1038018685985644851
  HOST_ID:8302c6d5-4c88-4695-bbf4-762bc7f24544
  RELEASE_VERSION:2.0.9
  LOAD:8.6713989E7
  RPC_ADDRESS:10.0.1.133
  SCHEMA:d9e0f9ae-6221-3987-81fb-1a0134e02b22
  SEVERITY:0.0
  DC:DC1
  NET_VERSION:7
  RACK:RAC2
/10.0.1.138
  generation:1410905966
  heartbeat:2160458
  STATUS:NORMAL,-1018547439030001277
  HOST_ID:c35a2261-a65f-4980-97a2-c4f82e034d4b
  RELEASE_VERSION:2.0.9
  LOAD:8.1261228E7
  RPC_ADDRESS:10.0.1.138
  SCHEMA:d9e0f9ae-6221-3987-81fb-1a0134e02b22
  SEVERITY:0.0
  DC:DC1
  NET_VERSION:7
  RACK:RAC1
/10.0.1.141
  generation:1411067164
  heartbeat:316859
  STATUS:NORMAL,-1019100682670215866
  HOST_ID:2a8dde5e-29b0-4a67-8204-40769376c44a
  RELEASE_VERSION:2.0.9
  LOAD:5.9687052E7
  RPC_ADDRESS:0.0.0.0
  SCHEMA:d9e0f9ae-6221-3987-81fb-1a0134e02b22
  SEVERITY:0.0
  DC:DC1
  NET_VERSION:7
  RACK:RAC1
/10.0.1.134
  generation:1411067353
  heartbeat:1671132
  STATUS:NORMAL,-1213104093606780526
  HOST_ID:eddb03b2-98d3-46ff-be63-95435414a883
  RELEASE_VERSION:2.0.9
  LOAD:9.7973556E7
  RPC_ADDRESS:10.0.1.134
  SCHEMA:d9e0f9ae-6221-3987-81fb-1a0134e02b22
  SEVERITY:0.0
  DC:DC1
  NET_VERSION:7
  RACK:RAC2
EOF

   my $str_describecluster = <<EOF;
Cluster Information:
	Name: Test Cluster
	Snitch: org.apache.cassandra.locator.DynamicEndpointSnitch
	Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
	Schema versions:
		d9e0f9ae-6221-3987-81fb-1a0134e02b22: [10.0.1.130, 10.0.1.133, 10.0.1.134, 10.0.1.136, 10.0.1.137, 10.0.1.138, 10.0.1.140]

		UNREACHABLE: [10.0.1.141]
EOF

   my $str_tpstats = <<EOF;
Pool Name                    Active   Pending      Completed   Blocked  All time blocked
ReadStage                         0         0              0         0                 0
RequestResponseStage              0         0        2622720         0                 0
MutationStage                     0         0        3681642         0                 0
ReadRepairStage                   0         0              0         0                 0
ReplicateOnWriteStage             0         0              0         0                 0
GossipStage                       0         0        1748251         0                 0
CacheCleanupExecutor              0         0              0         0                 0
MigrationStage                    0         0            594         0                 0
MemoryMeter                       0         0            909         0                 0
FlushWriter                       0         0            567         0                 0
ValidationExecutor                0         0           1296         0                 0
InternalResponseStage             0         0           3503         0                 0
AntiEntropyStage                  0         0           2592         0                 0
MemtablePostFlusher               0         0          12469         0                 0
MiscStage                         0         0            894         0                 0
PendingRangeCalculator            0         0             13         0                 0
CompactionExecutor                0         0           1850         0                 0
commitlog_archiver                0         0              0         0                 0
HintedHandoff                     0         0             11         0                 0

Message type           Dropped
RANGE_SLICE                  0
READ_REPAIR                  0
PAGED_RANGE                  0
BINARY                       0
READ                         0
MUTATION                     0
_TRACE                       0
REQUEST_RESPONSE             0
COUNTER_MUTATION             0
EOF

   my $str_cfstats = <<EOF;
Keyspace: system
	Read Count: 378
	Read Latency: 1.013100529100529 ms.
	Write Count: 24427
	Write Latency: 0.0698306791664961 ms.
	Pending Tasks: 0
		Table: IndexInfo
		SSTable count: 0
		Space used (live), bytes: 0
		Space used (total), bytes: 0
		SSTable Compression Ratio: 0.0
		Number of keys (estimate): 0
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 0
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 0
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 0
		Compacted partition minimum bytes: 0
		Compacted partition maximum bytes: 0
		Compacted partition mean bytes: 0
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: NodeIdInfo
		SSTable count: 1
		Space used (live), bytes: 5043
		Space used (total), bytes: 5043
		SSTable Compression Ratio: 1.0357142857142858
		Number of keys (estimate): 128
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 0
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 0
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 336
		Compacted partition minimum bytes: 51
		Compacted partition maximum bytes: 60
		Compacted partition mean bytes: 60
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: batchlog
		SSTable count: 0
		Space used (live), bytes: 0
		Space used (total), bytes: 0
		SSTable Compression Ratio: 0.0
		Number of keys (estimate): 0
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 0
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 0
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 0
		Compacted partition minimum bytes: 0
		Compacted partition maximum bytes: 0
		Compacted partition mean bytes: 0
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: compaction_history
		SSTable count: 3
		Space used (live), bytes: 54534
		Space used (total), bytes: 69678
		SSTable Compression Ratio: 0.45937778888709935
		Number of keys (estimate): 640
		Memtable cell count: 9
		Memtable data size, bytes: 3184
		Memtable switch count: 91
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 178
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 1648
		Compacted partition minimum bytes: 311
		Compacted partition maximum bytes: 535
		Compacted partition mean bytes: 431
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: compactions_in_progress
		SSTable count: 1
		Space used (live), bytes: 6019
		Space used (total), bytes: 22607
		SSTable Compression Ratio: 1.1875
		Number of keys (estimate): 128
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 111
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 111
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 1296
		Compacted partition minimum bytes: 30
		Compacted partition maximum bytes: 35
		Compacted partition mean bytes: 35
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: hints
		SSTable count: 0
		Space used (live), bytes: 0
		Space used (total), bytes: 0
		SSTable Compression Ratio: 0.0
		Number of keys (estimate): 0
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 0
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 0
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 0
		Compacted partition minimum bytes: 0
		Compacted partition maximum bytes: 0
		Compacted partition mean bytes: 0
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: local
		SSTable count: 2
		Space used (live), bytes: 21599
		Space used (total), bytes: 21848
		SSTable Compression Ratio: 0.45036450375387505
		Number of keys (estimate): 256
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 4
		Local read count: 11
		Local read latency: 0.000 ms
		Local write count: 7
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 1312
		Compacted partition minimum bytes: 11865
		Compacted partition maximum bytes: 14237
		Compacted partition mean bytes: 14237
		Average live cells per slice (last five minutes): 1.0
		Average tombstones per slice (last five minutes): 0.0

		Table: paxos
		SSTable count: 0
		Space used (live), bytes: 0
		Space used (total), bytes: 0
		SSTable Compression Ratio: 0.0
		Number of keys (estimate): 0
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 0
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 0
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 0
		Compacted partition minimum bytes: 0
		Compacted partition maximum bytes: 0
		Compacted partition mean bytes: 0
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: peer_events
		SSTable count: 2
		Space used (live), bytes: 45902
		Space used (total), bytes: 57502
		SSTable Compression Ratio: 0.4386755614416733
		Number of keys (estimate): 256
		Memtable cell count: 4
		Memtable data size, bytes: 5173
		Memtable switch count: 110
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 629
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 1312
		Compacted partition minimum bytes: 180
		Compacted partition maximum bytes: 61214
		Compacted partition mean bytes: 21671
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: peers
		SSTable count: 2
		Space used (live), bytes: 81697
		Space used (total), bytes: 81697
		SSTable Compression Ratio: 0.4220901380044729
		Number of keys (estimate): 256
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 1
		Local read count: 8
		Local read latency: 0.000 ms
		Local write count: 161
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 360
		Compacted partition minimum bytes: 18
		Compacted partition maximum bytes: 14237
		Compacted partition mean bytes: 13289
		Average live cells per slice (last five minutes): 1.0
		Average tombstones per slice (last five minutes): 0.0

		Table: range_xfers
		SSTable count: 0
		Space used (live), bytes: 0
		Space used (total), bytes: 0
		SSTable Compression Ratio: 0.0
		Number of keys (estimate): 0
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 0
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 0
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 0
		Compacted partition minimum bytes: 0
		Compacted partition maximum bytes: 0
		Compacted partition mean bytes: 0
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: schema_columnfamilies
		SSTable count: 2
		Space used (live), bytes: 28289
		Space used (total), bytes: 28289
		SSTable Compression Ratio: 0.17969921971921815
		Number of keys (estimate): 256
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 1
		Local read count: 35
		Local read latency: 0.000 ms
		Local write count: 32
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 360
		Compacted partition minimum bytes: 1332
		Compacted partition maximum bytes: 29521
		Compacted partition mean bytes: 9042
		Average live cells per slice (last five minutes): 1.0
		Average tombstones per slice (last five minutes): 0.0

		Table: schema_columns
		SSTable count: 2
		Space used (live), bytes: 31535
		Space used (total), bytes: 31535
		SSTable Compression Ratio: 0.19150107225036916
		Number of keys (estimate): 256
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 1
		Local read count: 58
		Local read latency: 0.000 ms
		Local write count: 32
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 360
		Compacted partition minimum bytes: 180
		Compacted partition maximum bytes: 61214
		Compacted partition mean bytes: 10839
		Average live cells per slice (last five minutes): 9.0
		Average tombstones per slice (last five minutes): 0.0

		Table: schema_keyspaces
		SSTable count: 2
		Space used (live), bytes: 10303
		Space used (total), bytes: 10303
		SSTable Compression Ratio: 0.5905360015138613
		Number of keys (estimate): 256
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 1
		Local read count: 0
		Local read latency: 0.000 ms
		Local write count: 2
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 352
		Compacted partition minimum bytes: 150
		Compacted partition maximum bytes: 215
		Compacted partition mean bytes: 204
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: schema_triggers
		SSTable count: 0
		Space used (live), bytes: 0
		Space used (total), bytes: 0
		SSTable Compression Ratio: 0.0
		Number of keys (estimate): 0
		Memtable cell count: 0
		Memtable data size, bytes: 0
		Memtable switch count: 0
		Local read count: 44
		Local read latency: 0.000 ms
		Local write count: 0
		Local write latency: 0.000 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 0
		Compacted partition minimum bytes: 0
		Compacted partition maximum bytes: 0
		Compacted partition mean bytes: 0
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

		Table: sstable_activity
		SSTable count: 1
		Space used (live), bytes: 7735
		Space used (total), bytes: 40519
		SSTable Compression Ratio: 0.288268156424581
		Number of keys (estimate): 128
		Memtable cell count: 423
		Memtable data size, bytes: 22402
		Memtable switch count: 148
		Local read count: 222
		Local read latency: 0.000 ms
		Local write count: 23275
		Local write latency: 0.046 ms
		Pending tasks: 0
		Bloom filter false positives: 0
		Bloom filter false ratio: 0.00000
		Bloom filter space used, bytes: 1296
		Compacted partition minimum bytes: 150
		Compacted partition maximum bytes: 179
		Compacted partition mean bytes: 179
		Average live cells per slice (last five minutes): 0.0
		Average tombstones per slice (last five minutes): 0.0

----------------
EOF

   my $str_compactionhistory = <<EOF;
b8553650-437e-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411517226549             3141           787            {4:17}
abb4d0c0-4051-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411168024524             2221           568            {4:12}
a2d15b80-41ab-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411316615480             2868           719            {4:15}
d2483090-43dc-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411557642777             3159           778            {4:17}
fc5c50c0-3f66-11e4-b7b9-27cc0cffe832     system             local                        1411067228108             6244           5723           {4:1}
277d7160-3f55-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411059569526             389            42             {2:2}
7c1e0480-4079-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411185124552             1755           764            {1:5, 2:17}
e84cab70-412d-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411262615463             2748           712            {1:1, 4:14}
603efc50-3f58-11e4-953d-27cc0cffe832     system             peers                        1411060953237             35886          35886          {1:7}
465ead20-4062-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411175155954             2434           628            {1:9, 4:12}
a70c00d0-42d0-11e4-b7b9-27cc0cffe832     system             peer_events                  1411442465117             29450          29212          {1:3, 4:1}
3200a4e0-41d5-11e4-b7b9-27cc0cffe832     system             peer_events                  1411334465070             25394          25230          {1:3, 4:1}
ec866660-4252-11e4-b7b9-27cc0cffe832     system             peer_events                  1411388465094             27485          27236          {1:3, 4:1}
5d5ac680-4229-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411370615528             2562           641            {4:13}
e131b000-431d-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411475633920             3139           782            {4:17}
378be200-3f08-11e4-953d-27cc0cffe832     system             peers                        1411026525216             35886          35886          {1:7}
bcf41070-40d9-11e4-b7b9-27cc0cffe832     system             peer_events                  1411226465015             21162          20939          {1:3, 4:1}
0e717440-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188806020             578            292            {1:3, 2:1}
4f3a72e0-3f58-11e4-953d-27cc0cffe832     system             schema_columnfamilies        1411060924686             9030           9030           {1:6}
82a4f140-4242-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411381415508             2569           636            {4:13}
2cdfd720-4125-11e4-b7b9-27cc0cffe832     system             peer_events                  1411258865042             22362          22121          {1:3, 4:1}
1fbdcce0-3f55-11e4-953d-27cc0cffe832     system             local                        1411059556526             5722           5722           {1:1}
8cef4e80-4322-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411477640040             390            42             {2:2}
373e3730-3f08-11e4-953d-27cc0cffe832     system             peer_events                  1411026524707             16688          16550          {1:1, 3:1}
200d9a90-3f55-11e4-953d-27cc0cffe832     system             schema_columns               1411059557049             7957           7957           {1:6}
f2910610-428d-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411413815537             2604           682            {1:1, 4:13}
4753ccc0-3f08-11e4-953d-27cc0cffe832     system             local                        1411026551692             5722           5722           {1:1}
3bcfc0d0-4006-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411135624541             2128           575            {1:1, 4:11}
f7b46240-43f5-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411568442980             3157           813            {4:17}
cd430ac0-4274-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411403015532             2588           645            {4:13}
13369d70-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188814023             464            146            {1:1, 2:2}
b3a84aa0-3f4d-11e4-953d-27cc0cffe832     system             peer_events                  1411056368714             18981          18787          {1:1, 4:2}
20337210-3f55-11e4-953d-27cc0cffe832     system             sstable_activity             1411059557297             1977           956            {1:14, 2:2, 3:9}
786614e0-40e2-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411230215470             2719           679            {4:14}
61231350-401f-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411146424581             2259           568            {4:12}
87c76cb0-42f2-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411457015547             2744           672            {4:14}
cbea62c0-3fba-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411103224556             2353           825            {1:9, 4:11}
c7397c80-4239-11e4-b7b9-27cc0cffe832     system             peer_events                  1411377665096             27093          26856          {1:3, 4:1}
1f0669f0-3f38-11e4-953d-27cc0cffe832     system             peer_events                  1411047099918             18316          18113          {1:1, 4:2}
0ee28590-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188806761             418            0              {2:3}
569429b0-4376-11e4-b7b9-27cc0cffe832     system             peer_events                  1411513626571             32060          31831          {1:3, 4:1}
37a7f580-3f08-11e4-953d-27cc0cffe832     system             schema_columns               1411026525400             27650          7957           {2:6}
b3cd37c0-3f4d-11e4-953d-27cc0cffe832     system             sstable_activity             1411056368956             2333           826            {1:10, 2:4, 3:1, 4:7}
21db3de0-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188838590             543            221            {1:2, 2:2}
9cc73100-4170-11e4-b7b9-27cc0cffe832     system             peer_events                  1411291265040             23543          23311          {1:3, 4:1}
2b0a7470-4452-11e4-b7b9-27cc0cffe832     system             peer_events                  1411608042807             35502          35270          {1:3, 4:1}
5750c310-41ee-11e4-b7b9-27cc0cffe832     system             peer_events                  1411345265089             25873          25618          {1:3, 4:1}
4fbcd0a0-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026565802             393            174            {1:2, 2:1}
ed74f340-41dd-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411338215540             2841           639            {1:2, 4:15}
4f535210-3f58-11e4-953d-27cc0cffe832     system             schema_keyspaces             1411060924849             374            374            {1:6}
2c4f92e0-3f55-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411059577614             396            42             {2:2}
c822d940-41c4-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411327415508             2841           704            {4:15}
47be7570-3f08-11e4-953d-27cc0cffe832     system             sstable_activity             1411026552391             1025           1025           {1:24}
087bd4d0-4097-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411197815453             4231           677            {1:129, 2:10, 3:13, 4:1}
20774110-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188836257             595            300            {1:3, 2:1}
127d1170-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188812807             616            216            {1:2, 2:2}
1ead5680-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188833256             552            228            {1:3, 2:1}
1f31bfb0-3f38-11e4-953d-27cc0cffe832     system             sstable_activity             1411047100203             2105           734            {1:9, 4:8}
206e0a10-3f55-11e4-953d-27cc0cffe832     system             schema_columnfamilies        1411059557681             9030           9030           {1:6}
2086e940-3f55-11e4-953d-27cc0cffe832     system             schema_keyspaces             1411059557844             374            374            {1:6}
0bfa2ee0-4344-11e4-b7b9-27cc0cffe832     system             peer_events                  1411492026574             31274          31029          {1:3, 4:1}
4b5ae170-43bb-11e4-b7b9-27cc0cffe832     system             peer_events                  1411543243015             33180          32956          {1:3, 4:1}
423e3070-4428-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411590042871             3256           805            {4:17}
4fec9e90-43bb-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411543250681             413            42             {2:3}
95e8a740-43ed-11e4-b7b9-27cc0cffe832     system             peer_events                  1411564842932             33976          33740          {1:3, 4:1}
8cd08a20-445a-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411611642818             3159           776            {4:17}
930bf5f0-4365-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411506426575             3141           783            {4:17}
16856f00-3fed-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411124824560             2074           524            {4:11}
50553b70-446b-11e4-b7b9-27cc0cffe832     system             peer_events                  1411618842791             35901          35669          {1:3, 4:1}
a7f33ab0-425b-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411392215515             2601           660            {4:13}
4e7f8750-3f58-11e4-953d-27cc0cffe832     system             peer_events                  1411060923461             18881          18881          {1:3}
7c9ac6c0-4207-11e4-b7b9-27cc0cffe832     system             peer_events                  1411356065068             26248          26050          {1:3, 4:1}
4f494e50-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026565045             389            42             {2:2}
e2458e30-40f2-11e4-b7b9-27cc0cffe832     system             peer_events                  1411237265043             21577          21338          {1:3, 4:1}
c21aaa90-4189-11e4-b7b9-27cc0cffe832     system             peer_events                  1411302065081             23942          23787          {1:3, 4:1}
bbd50eb0-4304-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411464833819             2992           783            {1:2, 2:5, 3:2, 4:12}
e7694220-41a2-11e4-b7b9-27cc0cffe832     system             peer_events                  1411312865090             24425          24288          {1:3, 4:1}
28119b10-3f55-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411059570497             392            135            {1:2, 2:1}
37d45cb0-3f08-11e4-953d-27cc0cffe832     system             sstable_activity             1411026525691             1826           1025           {1:15, 2:15}
fa3486b0-42f3-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411457637019             525            203            {1:2, 2:1}
678f3900-4441-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411600842896             3183           801            {4:17}
2da965c0-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188858396             505            148            {1:1, 2:2}
c3005dd0-4114-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411251815469             2696           675            {4:14}
401a4830-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026539571             392            153            {1:2, 2:1}
77825d70-4157-11e4-b7b9-27cc0cffe832     system             peer_events                  1411280465095             23152          22918          {1:3, 4:1}
bdd89290-4061-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411174926905             457            148            {1:1, 2:2}
10f42780-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188810232             534            216            {1:2, 2:1}
bb26eb20-4406-11e4-b7b9-27cc0cffe832     system             peer_events                  1411575642834             34370          34110          {1:3, 4:1}
e6a92650-432a-11e4-b7b9-27cc0cffe832     system             peer_events                  1411481226549             30851          30625          {1:3, 4:1}
3f9783a0-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026538714             393            42             {2:2}
4d63f7e0-43bb-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411543246430             526            287            {1:4}
11d3c570-426c-11e4-b7b9-27cc0cffe832     system             peer_events                  1411399265095             27861          27627          {1:3, 4:1}
17e039e0-42a7-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411424615550             2741           690            {4:14}
5c722d10-429e-11e4-b7b9-27cc0cffe832     system             peer_events                  1411420865121             28669          28433          {1:3, 4:1}
2dcda0b0-40b0-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411208615483             2695           667            {4:14}
74daff80-4078-11e4-b7b9-27cc0cffe832     system             peer_events                  1411184682872             19525          19334          {1:1, 2:1, 3:1, 4:1}
74909d40-3efd-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411021903124             533            211            {1:2, 2:1}
17a87680-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188821480             426            0              {2:3}
2372d7e0-3f50-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411057415262             515            243            {1:2, 2:1}
4ed1ed10-3f58-11e4-953d-27cc0cffe832     system             peers                        1411060924001             35886          35886          {1:7}
4739dc20-3f08-11e4-953d-27cc0cffe832     system             peer_events                  1411026551522             16550          16550          {1:2}
256e0190-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188844585             531            247            {1:2, 2:1}
bc075500-4061-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411174923856             524            213            {1:2, 2:1}
97b6b6f0-40c0-11e4-b7b9-27cc0cffe832     system             peer_events                  1411215665118             20759          20533          {1:3, 4:1}
81bc30c0-42b7-11e4-b7b9-27cc0cffe832     system             peer_events                  1411431665100             29061          28824          {1:3, 4:1}
81566860-3ef9-11e4-953d-27cc0cffe832     system             peer_events                  1411020206566             16525          16318          {1:1, 4:1}
74be01a0-4078-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411184682682             2583           633            {1:1, 2:3, 3:3, 4:10}
ace9f8c0-407a-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411185635916             409            159            {1:2, 2:1}
a12fd230-43a8-11e4-b7b9-27cc0cffe832     system             peer_events                  1411535226579             32884          32644          {1:3, 4:1}
31473fd0-435d-11e4-b7b9-27cc0cffe832     system             peer_events                  1411502826573             31666          31432          {1:3, 4:1}
3d378400-42c0-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411435415616             2755           683            {4:14}
478a9430-3f08-11e4-953d-27cc0cffe832     system             peers                        1411026552051             35886          35886          {1:7}
47f081f0-3f08-11e4-953d-27cc0cffe832     system             schema_keyspaces             1411026552719             374            374            {1:6}
6da47f30-434c-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411495626403             3151           787            {4:17}
3721e7d0-4285-11e4-b7b9-27cc0cffe832     system             peer_events                  1411410065101             28257          28034          {1:3, 4:1}
28d23b30-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188850275             536            153            {1:1, 2:2}
a9c99400-407c-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411186489664             551            244            {1:2, 2:1}
1c2943b0-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188829035             489            168            {1:1, 2:2}
37b67470-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026525495             295            0              {2:2}
4e97ca40-3f58-11e4-953d-27cc0cffe832     system             local                        1411060923620             5722           5722           {1:1}
4f05ce50-3f58-11e4-953d-27cc0cffe832     system             sstable_activity             1411060924341             956            956            {1:23}
05c72780-4439-11e4-b7b9-27cc0cffe832     system             peer_events                  1411597242872             35122          34877          {1:3, 4:1}
02e211c0-43b1-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411538826460             3169           801            {4:17}
b219a370-4473-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411622442791             3190           820            {2:1, 4:17}
48db6cf0-4062-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411175160127             407            42             {2:2}
814ce580-3f88-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411081624536             2382           808            {1:9, 4:11}
53db3280-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026572712             395            42             {2:2}
85e405e0-4322-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411477628222             453            148            {1:1, 2:2}
0cb8c410-41bc-11e4-b7b9-27cc0cffe832     system             peer_events                  1411323665105             24919          24769          {1:3, 4:1}
50c47ca0-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026567530             414            159            {1:2, 2:1}
725bc350-40a7-11e4-b7b9-27cc0cffe832     system             peer_events                  1411204865029             20350          20131          {1:3, 4:1}
a69c8e80-3fa1-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411092424552             2380           798            {1:9, 4:11}
1674b170-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188819463             597            300            {1:3, 2:1}
5317a460-40c9-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411219415462             2725           687            {4:14}
22750ce0-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188839598             495            148            {1:1, 2:2}
e3433640-407d-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411187015588             2717           588            {1:8, 2:4, 3:9, 4:6}
28cb0000-3f55-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411059571712             384            42             {2:2}
0da41220-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188804674             531            211            {1:3, 2:1}
f9e5e740-3f1e-11e4-953d-27cc0cffe832     system             sstable_activity             1411036300212             2859           734            {1:24, 2:15, 3:8}
12c20430-41f7-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411349015539             2566           635            {4:13}
486773d0-4333-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411484826509             3255           787            {1:2, 2:4, 3:4, 4:13}
a8aadd90-407c-11e4-b7b9-27cc0cffe832     system             peer_events                  1411186487785             19598          19403          {1:3, 4:1}
5c3144d0-3f6f-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411070824861             2736           815            {1:19, 2:14, 3:8}
32ffd270-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188867351             403            42             {2:2}
0d9e0220-4147-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411273415490             2855           716            {4:15}
3808faa0-4210-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411359815498             2553           639            {4:13}
20198170-3f55-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411059557127             327            0              {2:2}
1cec8ba0-440f-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411579242842             3256           827            {4:17}
41660210-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026541745             396            141            {1:2, 2:1}
7eadfe30-4079-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411185128851             417            42             {2:2}
32ea76d0-4160-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411284215485             2850           708            {4:15}
4eeacc40-3f58-11e4-953d-27cc0cffe832     system             schema_columns               1411060924164             7957           7957           {1:6}
40b528a0-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026540586             402            42             {2:2}
9db15110-40fb-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411241015457             2705           672            {4:14}
1ff3a9f0-3f55-11e4-953d-27cc0cffe832     system             peers                        1411059556879             35886          35886          {1:7}
29377d70-3f55-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411059572423             393            160            {1:2, 2:1}
146ad7b0-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188816043             517            219            {1:2, 2:1}
4532c180-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026548120             386            0              {2:2}
7be66ac0-438f-11e4-b7b9-27cc0cffe832     system             peer_events                  1411524426604             32465          32244          {1:3, 4:1}
dd90e220-4397-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411528026434             3134           775            {4:17}
38069040-3f08-11e4-953d-27cc0cffe832     system             schema_keyspaces             1411026526020             538            374            {1:5, 2:1}
37ed62f0-3f08-11e4-953d-27cc0cffe832     system             schema_columnfamilies        1411026525855             18671          9030           {2:6}
504588a0-3f08-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411026566698             409            64             {1:1, 2:2}
db8adce0-3f4d-11e4-953d-27cc0cffe832     system             compactions_in_progress      1411056435630             400            140            {1:2, 2:1}
72f98280-3efd-11e4-953d-27cc0cffe832     system             sstable_activity             1411021900456             3164           1020           {1:10, 2:4, 3:3, 4:11}
e5dbe280-407d-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411187019944             421            42             {1:1, 2:2}
4f20a950-3f58-11e4-953d-27cc0cffe832     system             NodeIdInfo                   1411060924517             62             62             {1:1}
1b3f0ac0-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188827500             567            241            {1:2, 2:1}
866c0590-4038-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411157224553             2230           549            {4:12}
e0705290-441f-11e4-b7b9-27cc0cffe832     system             peer_events                  1411586442809             34738          34488          {1:3, 4:1}
07962190-410c-11e4-b7b9-27cc0cffe832     system             peer_events                  1411248065065             21962          21731          {1:3, 4:1}
70883560-43d4-11e4-b7b9-27cc0cffe832     system             peer_events                  1411554042806             33585          33348          {1:3, 4:1}
62779ca0-42d9-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411446215530             2759           696            {4:14}
a69bd080-4078-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411184766344             408            166            {1:2, 2:1}
cc57d940-42e9-11e4-b7b9-27cc0cffe832     system             peer_events                  1411453265108             29842          29592          {1:3, 4:1}
47a56f30-3f08-11e4-953d-27cc0cffe832     system             schema_columns               1411026552227             7957           7957           {1:6}
7d842380-4192-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411305815480             2850           712            {4:15}
20515a50-3f55-11e4-953d-27cc0cffe832     system             NodeIdInfo                   1411059557493             62             62             {1:1}
58390e60-4179-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411295015494             2855           728            {4:15}
52407010-413e-11e4-b7b9-27cc0cffe832     system             peer_events                  1411269665169             22757          22523          {1:3, 4:1}
4d15b740-408e-11e4-b7b9-27cc0cffe832     system             peer_events                  1411194065076             19909          19725          {1:3, 4:1}
2b1d1590-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188854121             528            238            {1:2, 2:1}
0bb6c110-4082-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411188801441             405            164            {1:2, 2:1}
f9bd0280-3f1e-11e4-953d-27cc0cffe832     system             peer_events                  1411036299944             17497          17319          {1:1, 3:1, 4:1}
f13773b0-3fd3-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411114024555             2384           518            {1:9, 4:11}
a1e873f0-4220-11e4-b7b9-27cc0cffe832     system             peer_events                  1411366865071             26677          26453          {1:3, 4:1}
47d95070-3f08-11e4-953d-27cc0cffe832     system             schema_columnfamilies        1411026552567             9030           9030           {1:6}
5a479530-42fc-11e4-b7b9-27cc0cffe832     system             peer_events                  1411461234179             30103          29868          {1:3, 4:1}
ab3f9230-407c-11e4-b7b9-27cc0cffe832     system             compactions_in_progress      1411186492115             486            170            {1:1, 2:2}
1fa14430-3f55-11e4-953d-27cc0cffe832     system             peer_events                  1411059556339             18947          18881          {1:1, 2:2}
ac374a90-407a-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411185634745             2284           705            {1:4, 2:9, 3:10}
acfa0e30-43c3-11e4-b7b9-27cc0cffe832     system             sstable_activity             1411546842771             3315           797            {1:8, 4:17}
7f6be870-4315-11e4-b7b9-27cc0cffe832     system             peer_events                  1411472033911             30502          30289          {1:3, 4:1}
37565310-3f08-11e4-953d-27cc0cffe832     system             local                        1411026524865             5722           5722           {1:1}"
EOF

   my $str_netstats = <<EOF;
Mode: NORMAL
Not sending any streams.
Read Repair Statistics:
Attempted: 0
Mismatch (Blocking): 0
Mismatch (Background): 0
Pool Name                    Active   Pending      Completed
Commands                        n/a         0        2746282
Responses                       n/a         4        5128578
EOF

   my $str_ring = <<EOF;
Note: Ownership information does not include topology; for complete information, specify a keyspace

Datacenter: DC1
==========
Address     Rack        Status State   Load            Owns                Token                                       
                                                                           9218791492666026166                         
10.0.1.138  RAC1        Up     Normal  77.51 MB        12.00%              -9200596961513743511                        
10.0.1.138  RAC1        Up     Normal  77.51 MB        12.00%              -9193312131791122809                        
10.0.1.137  RAC2        Up     Normal  65.06 MB        11.90%              -9172043207297371463                        
10.0.1.130  RAC1        Up     Normal  87.26 MB        13.70%              -9167261364128570958                        
10.0.1.140  RAC1        Up     Normal  92.62 MB        12.77%              -9159578207340468218                        
10.0.1.138  RAC1        Up     Normal  77.51 MB        12.00%              -9158736675336169399                        
10.0.1.130  RAC1        Up     Normal  87.26 MB        13.70%              -9145694463085451844                        
10.0.1.137  RAC2        Up     Normal  65.06 MB        11.90%              -9138636967913758778                        
10.0.1.134  RAC2        Up     Normal  93.44 MB        12.16%              -9127695775998757296                        
10.0.1.136  RAC2        Up     Normal  68.28 MB        12.92%              -9121916472245695245                        
10.0.1.130  RAC1        Up     Normal  87.26 MB        13.70%              -9121563762631804279                        
10.0.1.136  RAC2        Up     Normal  68.28 MB        12.92%              -9112464936673978643                        
10.0.1.134  RAC2        Up     Normal  93.44 MB        12.16%              -9109069514593226311                        
10.0.1.140  RAC1        Up     Normal  92.62 MB        12.77%              -9090960695561522598
EOF

   my $str_ring_system = <<EOF;

Datacenter: DC1
==========
Address     Rack        Status State   Load            Owns                Token                                       
                                                                           9218791492666026166                         
10.0.1.138  RAC1        Up     Normal  77.51 MB        0.00%               -9200596961513743511                        
10.0.1.138  RAC1        Up     Normal  77.51 MB        0.00%               -9193312131791122809                        
10.0.1.137  RAC2        Up     Normal  65.06 MB        0.00%               -9172043207297371463                        
10.0.1.130  RAC1        Up     Normal  87.26 MB        0.00%               -9167261364128570958                        
10.0.1.140  RAC1        Up     Normal  92.62 MB        100.00%             -9159578207340468218                        
10.0.1.138  RAC1        Up     Normal  77.51 MB        0.00%               -9158736675336169399                        
10.0.1.130  RAC1        Up     Normal  87.26 MB        0.00%               -9145694463085451844                        
10.0.1.137  RAC2        Up     Normal  65.06 MB        0.00%               -9138636967913758778                        
10.0.1.134  RAC2        Up     Normal  93.44 MB        0.00%               -9127695775998757296                        
10.0.1.136  RAC2        Up     Normal  68.28 MB        0.00%               -9121916472245695245                        
10.0.1.130  RAC1        Up     Normal  87.26 MB        0.00%               -9121563762631804279                        
10.0.1.136  RAC2        Up     Normal  68.28 MB        0.00%               -9112464936673978643                        
10.0.1.134  RAC2        Up     Normal  93.44 MB        0.00%               -9109069514593226311                        
10.0.1.140  RAC1        Up     Normal  92.62 MB        100.00%             -9090960695561522598                        
10.0.1.140  RAC1        Up     Normal  92.62 MB        100.00%             -9077115458327522696 
EOF

   my $str_statusbinary = "running\n";

   my $str_statusthrift = "running\n";

   my $str_getLoggingLevels = <<EOF;
Logger Name                                        Log Level
root                                                    INFO
org.apache.thrift.server.TNonblockingServer            ERROR
EOF

# main

   my $args = process_args();

   # usually the args will be "command keyspace cf"
   my ($c, $keyspace, $cfstats) = split / +/, $args;

   if (not defined $c or $c eq "") {
      print "Command was not specified.\n";
      print $str_help;
      exit 1;
   }

   $keyspace = 'system';

   if (exists $opts{host} and $opts{host} eq "bad") {
      print "Cannot resolve 'bad': unknown host\n";
      print $str_help;
      exit 1;
   }

   # Big Switch

   if    ($c eq "cfstats"            ) { print $str_cfstats; }
   elsif ($c eq "compactionhistory"  ) { print $str_compactionhistory; }
   elsif ($c eq "compactionstats"    ) { print $str_compactionstats; }
   elsif ($c eq "describecluster"    ) { print $str_describecluster; }
   elsif ($c eq "help"               ) { print $str_help; }
   elsif ($c eq "info" and $opts{'tokens'}) { print $str_info_T; }
   elsif ($c eq "info"               )  { print $str_info; }
   elsif ($c eq "status"             ) { print $str_status; }
   elsif ($c eq "getcompactionthroughput") { print $str_getcompactionthroughput; }
   elsif ($c eq "getLoggingLevels"   ) { print $str_getLoggingLevels; }
   elsif ($c eq "getstreamthroughput") { print $str_getstreamthroughput; }
   elsif ($c eq "gossipinfo"         ) { print $str_gossipinfo; }
   elsif ($c eq "netstats"           ) { print $str_netstats; }
   elsif ($c eq "ring" and $keyspace eq 'system') { print $str_ring_system; }
   elsif ($c eq "ring"               ) { print $str_ring; }
   elsif ($c eq "statusbinary"       ) { print $str_statusbinary; }
   elsif ($c eq "statusthrift"       ) { print $str_statusthrift; }
   elsif ($c eq "tpstats"            ) { print $str_tpstats; }
   elsif ($c eq "version"            ) { print $str_version; }
   else {
      # the command must be a non-reporting command, so just display it
      print "nodetool $args\n";
   }

   exit 0;

sub process_args {

# -a,--include-all-sstables   includes sstables that are already on the
#                             most recent version during upgradesstables
# -c,--compact                print histograms in a more compact format
# -cf,--column-family <arg>   only take a snapshot of the specified table
#                             (column family)
# -dc,--in-dc <arg>           only repair against nodes in the specified
#                             datacenters (comma separated)
# -et,--end-token <arg>       token at which repair range ends
# -h,--host <arg>             node hostname or ip address
# -hosts,--in-host <arg>      only repair against specified nodes (comma
#                             separated)
# -i,--ignore                 ignore the supplied list of
#                             keyspace.columnfamiles in statistics
# -local,--in-local-dc        only repair against nodes in the same
#                             datacenter
# -ns,--no-snapshot           disables snapshot creation for scrub
# -p,--port <arg>             remote jmx agent port number
# -par,--parallel             repair nodes in parallel.
# -pr,--partitioner-range     only repair the first range returned by the
#                             partitioner for the node
# -pw,--password <arg>        remote jmx agent password
# -r,--resolve-ip             show node domain names instead of IPs
# -s,--skip-corrupted         when scrubbing counter tables, skip corrupted
#                             rows
# -st,--start-token <arg>     token at which repair range starts
# -T,--tokens                 display all tokens
# -t,--tag <arg>              optional name to give a snapshot
# -u,--username <arg>         remote jmx agent username

   # create the options spec for Getopt::Long. Use : before the args type so that our validation sub is called.
   my @spec = ("include-all-sstables|a",
               "column-family|cf:s",
               "compact|c",
               "in-dc|dc:s",
               "host|h:s",
               "hosts|in-host:s",
               "ignore|i",
               "local|in-local",
               "no-snapshot|ns",
               "parallel|par",
               "partitioner-range|pr",
               "port|p:i",
               "resolve-ip|r",
               "skip-corrupted|s",
               "tag|t:s",
               "tokens|T",
               "username|u:s",
               "password|pw:s",
               "start-token|st:s",
               "end-token|et:s",
   );

   my @options;
   
   for my $s (@spec) {
      if (my ($long, $short, $type) = $s =~ /([\w-]+)\|([\w]+)[=:]?([fios]*)/i) {
         push @options, ($s, $type ne '' ? \&value_required : \&value_not_required);
      }
      else {
         die "bad spec: $s";
      }
   }

   print Dumper(@options) if $DEBUG;

   # args pre-processing - to reduce parsing ambiguities, replace some of the short options with long options before calling Getopt::Long.
   # Users should use double dashes if they want long args to really get parsed right, instead of as bundled options.
   my %not_so_short_options = ( cf  => "column-family",
                                dc  => "in-dc",
                                et  => "end-token",
                                par => "parallel",
                                pr  => "partitioner-range",
                                pw  => "password",
                                st  => "start-token",
   );
 
#   if (scalar(@ARGV) > 0) {
#      for (my $n=0; $n < scalar(@ARGV); $n++) {
#          $ARGV[$n] =~ s/^(--?)([\w-]{2,3})$/exists $not_so_short_options{$2} ? "--$not_so_short_options{$2}" : "$1$2"/e;
#      }
#   }

   my $result = GetOptions(@options);

   if ($result == 0 or $go_err > 0) {
      print $str_help;
      exit 1;
   }

   # args post-processing
   if (scalar(@ARGV) > 0 and $ARGV[0] eq '--') {
      shift @ARGV;
   }

   my $cmd = join(' ', @ARGV);
   $cmd =~ s/^ +//;
   $cmd =~ s/ +$//;
   $cmd =~ s/ +/ /g;

   if ($DEBUG) {
      print Dumper(\%opts);
      print "cmd=$cmd\n";
   }

   return $cmd;
}

sub value_not_required {
# option arg not expected, but we still want to set it to 1
   my ($option, $value, $rhash) = @_;

   if ($option ne "") {
      $opts{$option} = 1;
   }
}

sub value_required {
# option arg expected, do error handling if missing, including a custom error message
   my ($option, $value, $rhash) = @_;

   print "in value_required: $option, $value\n" if $DEBUG;

   if ($option ne "") {
      if (not defined $value or $value eq "") {
         print "Missing argument for option:$option\n";
         $go_err++;
         die("!FINISH");
      }
      else {
         if (exists $opts{$option}) {
            print "Unrecognized command: $value\n";
            $go_err++;
            die("!FINISH");
         }
         else {
            $opts{$option} = $value;
         }
      }
   }
}
