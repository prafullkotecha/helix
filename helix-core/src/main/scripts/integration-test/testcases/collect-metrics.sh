#!/bin/bash

DELAY=1
NOW=`date +%s`
TODAY=`date +%Y-%m-%d`
EOD=`date --utc --date "$TODAY 23:59:59" +%s`
TOEOD=`expr \( $EOD - $NOW \)`
COUNT=`expr $TOEOD \/ $DELAY`
INTERVAL=$DELAY

for ((i = 0; i < 30; i++))
do
  t=`date +"%Y-%m-%d"`
  export RESULT="./sar-results-$t"

  mkdir -p $RESULT

  top -b -M -n $COUNT -d $INTERVAL  > $RESULT/top.out &
  sar -B $INTERVAL $COUNT > $RESULT/sar.paging.out &
  sar -d -p $INTERVAL $COUNT > $RESULT/sar.device.out &
  sar -m $INTERVAL $COUNT > $RESULT/sar.cpuhz.out &
  sar -R $INTERVAL $COUNT > $RESULT/sar.memory.out &
  sar -r $INTERVAL $COUNT > $RESULT/sar.memutil.out &
  sar -u ALL $INTERVAL $COUNT > $RESULT/sar.cpuusage.out &
  sar -W $INTERVAL $COUNT > $RESULT/sar.swapping.out &
  sar -w $INTERVAL $COUNT > $RESULT/sar.csw.out &
  swapon -s > $RESULT/swapon.out &
  cat /proc/sys/vm/min_free_kbytes >> $RESULT/min_free_kbytes.out &
  cat /proc/sys/vm/swappiness >> $RESULT/swappiness.out &

  sleep `expr $TOEOD + 3`
  TOEOD=`expr 3600 \* 24 `
  COUNT=`expr $TOEOD \/ $DELAY`

done
