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

cecho "trying to skip tar local m2 repo and helix jar" $red
# : <<'END'
cecho "tar local m2 repo and helix jar.." $green
cd
echo "tar m2 repo"
tar jcf /tmp/m2.tar.bz2 .m2
cd ~/temp/workspace
echo "tar helix"
tar jcf /tmp/helix.tar.bz2 helix

echo "make a clean build of helix"
cd /tmp
tar jxf helix.tar.bz2
cd /tmp/helix
mvn clean
mvn package -Dmaven.test.skip.exec=true
cd /tmp
tar jcf helix.tar.bz2 helix
# END

#: <<'END'
for i in `seq 0 $(($machine_nb-1))`; do
  cd /tmp
  echo "copy m2 repo to ${MACHINE_TAB[$i]}"
  scp m2.tar.bz2 ${USER_TAB[$i]}@${MACHINE_TAB[$i]}:~/
  echo "copy helix tar to ${MACHINE_TAB[$i]}"
  scp helix.tar.bz2 ${USER_TAB[$i]}@${MACHINE_TAB[$i]}:~/workspace/

  echo "overwrite m2 repo on ${MACHINE_TAB[$i]}"
  ssh ${USER_TAB[$i]}@${MACHINE_TAB[$i]} "rm -rf .m2"
  ssh ${USER_TAB[$i]}@${MACHINE_TAB[$i]} "tar jxf m2.tar.bz2"
  echo "overwrite helix on ${MACHINE_TAB[$i]}"
  ssh ${USER_TAB[$i]}@${MACHINE_TAB[$i]} "rm -rf workspace/helix"
  ssh ${USER_TAB[$i]}@${MACHINE_TAB[$i]} "cd workspace;tar jxf helix.tar.bz2"

done
#END
