#!/bin/sh
cd ../../../../../
mvn clean package -Dmaven.test.skip.exec=true
cd -
chmod +x ../../../../../target/helix-core-pkg/bin/
if [ 0 ];then
rsync -r /home/kgopalak/.m2 eat1-app05.corp:~/ &
rsync -r /home/kgopalak/.m2 eat1-app06.corp:~/ &
rsync -r /home/kgopalak/.m2 eat1-app07.corp:~/ &
rsync -r /home/kgopalak/.m2 eat1-app08.corp:~/ &
rsync -r /home/kgopalak/.m2 eat1-app09.corp:~/ &
rsync -r /home/kgopalak/.m2 eat1-app10.corp:~/ &
fi

cd ../../../../../../../
rsync --exclude="*.log" -r helix eat1-app207.corp:~/workspace &
rsync --exclude="*.log" -r helix eat1-app05.corp:~/workspace  &
rsync --exclude="*.log" -r helix eat1-app06.corp:~/workspace  &
rsync --exclude="*.log" -r helix eat1-app07.corp:~/workspace  &
rsync --exclude="*.log" -r helix eat1-app08.corp:~/workspace  &
rsync --exclude="*.log" -r helix eat1-app09.corp:~/workspace  &
rsync --exclude="*.log" -r helix eat1-app10.corp:~/workspace &

sleep 5
cd -

pending=`jobs -l | grep rsync  | awk '{print $2}'  | wc -l `
while  [ $pending -gt 1 ]
do
  pending=`jobs -l | grep rsync  | awk '{print $2}'  | wc -l `
  echo $pending
  sleep 3;
done

