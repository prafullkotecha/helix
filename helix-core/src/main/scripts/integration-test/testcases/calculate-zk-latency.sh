#!/bin/bash
cd ../var/log/helix_random_kill_remote

echo "participant zk stats:"
echo -e "operation\t sum\t count\t avg"
echo -e "----------------------------------------"
ls *process_start*.log -1rt | tail -n 1 | xargs grep "create\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "create\t\t " sum "\t " count "\t " sum/count}'

ls *process_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "read\t\t " sum "\t " count "\t " sum/count}'

ls *process_start*.log -1rt | tail -n 1 | xargs grep "update\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "update\t\t " sum "\t " count "\t " sum/count}'

ls *process_start*.log -1rt | tail -n 1 | xargs grep "delete\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "delete\t\t " sum "\t "count "\t " sum/count}'


ls *process_start*.log -1rt | tail -n 1 | xargs grep "getChildren\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "readChild\t " sum "\t " count "\t " sum/count}'


ls *process_start*.log -1rt | tail -n 1 | xargs grep "exists\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "exist\t\t " sum "\t " count "\t " sum/count}'

ls *process_start*.log -1rt | tail -n 1 | xargs grep "getStat\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "getStat\t\t " sum "\t " count "\t " sum/count}'

echo -e "\ncontroller zk stats:"
echo -e "operation\t sum\t count\t avg"
echo -e "----------------------------------------"
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "asyncCreate\. path" | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "asyncCreate\t " sum "\t " count "\t " sum/count}'

ls *manager_start*.log -1rt | tail -n 1 | xargs grep "create\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "create\t\t " sum "\t " count "\t " sum/count}'

ls *manager_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "read\t\t " sum "\t " count "\t " sum/count}'

ls *manager_start*.log -1rt | tail -n 1 | xargs grep "update\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "update\t\t " sum "\t " count "\t " sum/count}'

ls *manager_start*.log -1rt | tail -n 1 | xargs grep "delete\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "delete\t\t " sum "\t "count "\t " sum/count}'


ls *manager_start*.log -1rt | tail -n 1 | xargs grep "getChildren\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "readChild\t " sum "\t " count "\t " sum/count}'


ls *manager_start*.log -1rt | tail -n 1 | xargs grep "exists\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "exist\t\t " sum "\t " count "\t " sum/count}'

ls *manager_start*.log -1rt | tail -n 1 | xargs grep "getStat\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "getStat\t\t " sum "\t " count "\t " sum/count}'

