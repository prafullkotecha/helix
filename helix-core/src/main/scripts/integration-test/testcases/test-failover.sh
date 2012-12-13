[ -z $STORAGENODE_HOSTS ] && STORAGENODE_HOSTS="eat1-app85.corp eat1-app86.corp"
[ -z $STORAGENODE_GLUPATH ] && STORAGENODE_GLUPATH="/export/content/glu/apps/espresso-storage-node/i001"
[ -z $ZOOKEEPER_HOST_PORT ] && ZOOKEEPER_HOST_PORT="eat1-app87.corp:2181"
[ -z $ZOOKEEPER_HOST ] && ZOOKEEPER_HOST="eat1-app87.corp"
[ -z $ZKCLIENT_DIR ] && ZKCLIENT_DIR=$HOME/bin/zookeeper-3.3.3/bin
[ -z $SLEEP_TIME_FOR_FAILOVER ] && SLEEP_TIME_FOR_FAILOVER=5
[ -z $SLEEP_TIME_TO_RESTART ] && SLEEP_TIME_TO_RESTART=10
[ -z $ZKLOGPATH ] && ZKLOGPATH=/mnt/u001/zookeeper/data/version-2

# test

# get storage-node to fail
STORAGE_NODE_TO_FAIL=$( $ZKCLIENT_DIR/zkCli.sh -server $ZOOKEEPER_HOST_PORT get /ESPRESSO_DEV_SANDBOX_2/EXTERNALVIEW/MyDB 2>&1 | grep -m1 "MASTER" | sed 's/.*eat/eat/g' | sed 's/\.linkedin.*//g' )
echo "STORAGE_NODE_TO_FAIL=$STORAGE_NODE_TO_FAIL"

# get the session-id of the storage-node to fail
SESSION_ID_TO_FAIL=$( $ZKCLIENT_DIR/zkCli.sh -server $ZOOKEEPER_HOST_PORT stat /ESPRESSO_DEV_SANDBOX_2/LIVEINSTANCES/$STORAGE_NODE_TO_FAIL.linkedin.com_21601 2>&1 | grep "ephemeralOwner" | sed 's/.*0x//g' )
echo "SESSION_ID_TO_FAIL=$SESSION_ID_TO_FAIL"

# output dir
dt=$( date +"%m%d_%H%M%S" )
RESULTBASE=$PWD/FailoverTest_$dt
echo "RESULTBASE=$RESULTBASE"

echo "STORAGENODE_HOSTS=$STORAGENODE_HOSTS"
echo "STORAGENODE_GLUPATH=$STORAGENODE_GLUPATH"
echo "ZOOKEEPER_HOST=$ZOOKEEPER_HOST"
echo "ZOOKEEPER_HOST_PORT=$ZOOKEEPER_HOST_PORT"
# echo "STORAGE_NODE_TO_FAIL=$STORAGE_NODE_TO_FAIL"
echo "SLEEP_TIME_FOR_FAILOVER=$SLEEP_TIME_FOR_FAILOVER"
echo "SLEEP_TIME_TO_RESTART=$SLEEP_TIME_TO_RESTART"
# echo "OUTDIR=$OUTDIR"

#dt=$( date +%FT%T )
#RESULTBASE=$OUTDIR/FailoverTest_$dt
mkdir -p $RESULTBASE

########### TAIL LOG -> SLEEP -> FAIL STORAGE NODE -> SLEEP -> STOP TAIL LOG ########
for sn in $STORAGENODE_HOSTS
do
	ssh $sn tail -f $STORAGENODE_GLUPATH/logs/espresso-storage-node.log > $RESULTBASE/Sn_Participant.$sn.log &
done

[ ! -z $SLEEP_TIME_FOR_FAILOVER ] && sleep $SLEEP_TIME_FOR_FAILOVER

ssh $STORAGE_NODE_TO_FAIL "sudo -u app $STORAGENODE_GLUPATH/bin/container-jettyctl.sh stop"

# KILL_START_TIME=$( expr substr "$( date +%s%N )" 1 13 )
echo -n "[test-failover.sh] Killed storage node $STORAGE_NODE_TO_FAIL, at " >> $RESULTBASE/summary.txt && date -u >> $RESULTBASE/summary.txt

sleep $SLEEP_TIME_TO_RESTART

for sn in $STORAGENODE_HOSTS
do
	pid=$( ssh $sn ps -ef | grep tail | grep -v grep | grep $( whoami ) | awk '{ print $2 }' )
	ssh $sn kill $pid
done

#########################################
# compute the failover latency here processing here
#########################################
echo "STORAGE NODE FAILOVER LATENCY COMPUTATION: " >> $RESULTBASE/summary.txt

zkLogs=$( ssh $ZOOKEEPER_HOST ls -alt $ZKLOGPATH/log.* | awk '{ if ( NF == 9 ) print $9 }' | head -n 1 )
for zkLog in $zkLogs
do 
#	if [ "$zkLog" != "." ]; then
		echo "[processZKFailOver.sh] Processing $zkLog on $ZOOKEEPER_HOST with zk-log-parser."
		# filename=$( echo "zkLog" | sed 's/.*log/log/g' )
		
		ssh $ZOOKEEPER_HOST "export PATH=\$PATH:/export/apps/jdk/JDK-1_6_0_21/bin; /export/home/zzhang/helix/helix-core/target/helix-core-pkg/bin/zk-log-parser log $zkLog" > $RESULTBASE/$ZOOKEEPER_HOST.zkLog.parsed
		# find close-session time
		firstLine=$( grep "closeSession" $RESULTBASE/$ZOOKEEPER_HOST.zkLog.parsed | grep $SESSION_ID_TO_FAIL )
		echo "closeSession: $firstLine" >> $RESULTBASE/summary.txt
		if [ "$firstLine" != "" ]; then
			firstTime=$( echo $firstLine | awk '{ print $1 }' | awk -F ":" '{ print $2 }' )
			echo "On $ZOOKEEPER_HOST, closeSession was seen at time $firstTime" >> $RESULTBASE/summary.txt
			
			# tail zkLog file
			grep -A10000 "time:$firstTime" $RESULTBASE/$ZOOKEEPER_HOST.zkLog.parsed > $RESULTBASE/$ZOOKEEPER_HOST.zkLog.parsed.part
			line=$( grep create $RESULTBASE/$ZOOKEEPER_HOST.zkLog.parsed.part | grep MESSAGE | wc -l )
			echo -e "createMsg:\t$line" >> $RESULTBASE/summary.txt
			line=$( grep setData $RESULTBASE/$ZOOKEEPER_HOST.zkLog.parsed.part | grep CURRENTSTATE | wc -l )
			echo -e "setCurState:\t$line" >> $RESULTBASE/summary.txt
			line=$( grep setData $RESULTBASE/$ZOOKEEPER_HOST.zkLog.parsed.part | grep EXTERNALVIEW | wc -l )
			echo -e "setExtView:\t$line" >> $RESULTBASE/summary.txt
		fi
			
		# find last-cs-update time
		lastLine=$( cat $RESULTBASE/$ZOOKEEPER_HOST.zkLog.parsed.part | grep setData | grep ESPRESSO_DEV_SANDBOX_2 | grep MyDB | grep CURRENTSTATE | tail -n 1 )
		if [ "$lastLine" != "" ]; then
			lastTime=$( echo $lastLine | awk '{ print $1 }' | awk -F ":" '{ print $2 }' )
			echo "On $ZOOKEEPER_HOST, the last ZK setData CURRENTSTATE was seen at time $lastTime" >> $RESULTBASE/summary.txt
			break;	
		fi
#	fi  	
done

latency=$(( $lastTime-$firstTime ))
echo "FAILOVER LATENCY=$latency ms" >> $RESULTBASE/summary.txt

#########################################
# start the node you killed
#########################################
ssh $STORAGE_NODE_TO_FAIL "sudo -u app $STORAGENODE_GLUPATH/bin/container-jettyctl.sh start"
echo -n "[startFailOverTests.v1.0.sh] Started back storage node $STORAGE_NODE_TO_FAIL, at " >> $RESULTBASE/summary.txt && date -u >> $RESULTBASE/summary.txt

cat $RESULTBASE/summary.txt


