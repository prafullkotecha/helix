#!/bin/bash
# users/machines/dirs info for each test machine
USER_TAB=( "zzhang" "zzhang" "zzhang" )
# MACHINE_TAB=( "eat1-app20.corp" "eat1-app21.corp" "eat1-app22.corp" )
MACHINE_TAB=( "eat1-app26.corp" "eat1-app27.corp" "eat1-app28.corp" )

# SCRIPT_DIR_TAB=( "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" "/export/home/zzhang/workspace/helix/helix-core/src/main/scripts/integration-test/script" )

# constants
machine_nb=${#MACHINE_TAB[*]}

# colorful echo
red='\e[00;31m'
green='\e[00;32m'
function cecho
{
  message="$1"
  if [ -n "$message" ]; then
    color="$2"
    if [ -z "$color" ]; then
      echo "$message"
    else
      echo -e "$color$message\e[00m"
    fi
  fi
}

# : <<'END'
cecho "get zk logs" $green
cd /home/zzhang/temp/workspace/helix/helix-core/target/helix-core-pkg
output_dir=zklog_`date +"%y%m%d_%H%M%S"`
mkdir $output_dir

for i in `seq 0 $(($machine_nb-1))`; do
  zk_log=`ssh ${USER_TAB[$i]}@${MACHINE_TAB[$i]} "find workspace/helix/helix-core/src/main/scripts/integration-test/var/log/helix_random_kill_remote/zookeeper_data -name log.*"`
  # echo "$zk_log"

  echo "copy $zk_log from ${MACHINE_TAB[$i]}"
  zk_log_output=$output_dir/log.100000001-${MACHINE_TAB[$i]}.log
  # echo "$zk_log_output"
  scp ${USER_TAB[$i]}@${MACHINE_TAB[$i]}:$zk_log $zk_log_output
  bin/zk-log-parser log $zk_log_output > $zk_log_output.parsed
  if [ $i == 0 ]; then
    # echo "$i"
    bin/analyze-zk-log $zk_log_output.parsed test-cluster
  fi
done

# END
