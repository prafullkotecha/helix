#!/bin/bash
# users/machines/dirs info for each test machine
USER_TAB=( "zzhang" "zzhang" "zzhang" "zzhang" )
# MACHINE_TAB=( "eat1-app20.corp" "eat1-app21.corp" "eat1-app22.corp" "eat1-app84.corp" )
MACHINE_TAB=( "eat1-app26.corp" "eat1-app27.corp" "eat1-app28.corp" "eat1-app29.corp" )

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

echo "build helix"
cd /home/zzhang/workspace3/helix
rm -rf helix-core/target/*.jar
mvn package -Dmaven.test.skip.exec=true
cd helix-core/target
chmod +x helix-core-pkg/bin/*
tar jcf helix-core-pkg.tar.bz2 helix-core-pkg

cecho "files to copy" $green
cd /home/zzhang/workspace3/helix
ls helix-core/target/*.jar
ls helix-core/target/*.bz2

# : <<'END'
for i in `seq 0 $(($machine_nb-1))`; do
  echo "copy helix-core jars to ${MACHINE_TAB[$i]}"
  cd /home/zzhang/workspace3/helix/helix-core/target
  scp *.jar ${USER_TAB[$i]}@${MACHINE_TAB[$i]}:~/workspace/helix/helix-core/target/

  echo "copy helix-core-pkg to ${MACHINE_TAB[$i]}"
  scp helix-core-pkg.tar.bz2 ${USER_TAB[$i]}@${MACHINE_TAB[$i]}:~/workspace/helix/helix-core/target/
  ssh ${USER_TAB[$i]}@${MACHINE_TAB[$i]} "rm -rf workspace/helix/helix-core/target/helix-core-pkg"
  ssh ${USER_TAB[$i]}@${MACHINE_TAB[$i]} "cd workspace/helix/helix-core/target;tar jxf helix-core-pkg.tar.bz2"

#  cecho "remove all logs" $red
#  ssh ${USER_TAB[$i]}@${MACHINE_TAB[$i]} "rm -rf workspace/helix/helix-core/src/main/scripts/integration-test/var/log/*"
done
# END
