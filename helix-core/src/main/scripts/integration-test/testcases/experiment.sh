#!/bin/sh
#./setup_remote.sh
P="3600"
N="6"
T="1 5 25 50"
for partitions in $P
do
  for nodes in $N
  do
    for parallelism in $T
    do 
      #we have 6 nodes to run the experiment 
      instances_per_node=`expr $nodes / 6` 
      echo "parallel=$parallelism partitions=$partitions instances_per_node=$instances_per_node"
      echo srst | nc eat1-app205.corp 2191
      echo srst | nc eat1-app206.corp 2191
      echo srst | nc eat1-app207.corp 2191
      ./remote.test $parallelism $partitions $instances_per_node
      ssh eat1-app207.corp  "cd /export/home/eng/kgopalak/workspace/helix/helix-core/src/main/scripts/integration-test/testcases/;./analyze-zklog_local.sh ~/zookeeper/server1/data/ > helix_perf_${partitions}_${nodes}_${parallelism}`date '+%y%m%d_%H%m%d%s.log'`"
      ssh eat1-app207.corp  "ls -ltr /tmp/helix_perf_* | tail -1 | awk '{print \$9}' | xargs grep latency" 
    done
  done
done
