#!/bin/bash
cd ../var/log/helix_random_kill_remote

# participant stats
echo "participant zk stats:"
echo -e "operation\t sum\t count\t avg"
echo -e "----------------------------------------"
ls *process_start*.log -1rt | tail -n 1 | xargs grep "create\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "create\t\t " sum "\t " count "\t " sum/count}'

# read stats
ls *process_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "read\t\t " sum "\t " count "\t " sum/count}'
ls *process_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep CURRENTSTATES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{ if(count > 0) print "  read CS\t " sum "\t " count "\t " sum/count; else print "  read CS\t " 0 }'
ls *process_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep MESSAGES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  read MSG\t " sum "\t " count "\t " sum/count}'
ls *process_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep LIVEINSTANCES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  read LI\t " sum "\t " count "\t " sum/count}'
ls *process_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep STATEMODELDEFS | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  read SM\t " sum "\t " count "\t " sum/count}'

# update stats
ls *process_start*.log -1rt | tail -n 1 | xargs grep "update\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "update\t\t " sum "\t " count "\t " sum/count}'
ls *process_start*.log -1rt | tail -n 1 | xargs grep "update\." | grep MESSAGES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  update MSG\t " sum "\t " count "\t " sum/count}'
ls *process_start*.log -1rt | tail -n 1 | xargs grep "update\." | grep CURRENTSTATES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{ if (count > 0) print "  update CS\t " sum "\t " count "\t " sum/count; else print "  update CS\t " 0 }'



# delete stats
ls *process_start*.log -1rt | tail -n 1 | xargs grep "delete\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "delete\t\t " sum "\t "count "\t " sum/count}'
ls *process_start*.log -1rt | tail -n 1 | xargs grep "delete\." | grep MESSAGES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  delete MSG\t " sum "\t "count "\t " sum/count}'

ls *process_start*.log -1rt | tail -n 1 | xargs grep "getChildren\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "readChild\t " sum "\t " count "\t " sum/count}'


ls *process_start*.log -1rt | tail -n 1 | xargs grep "exists\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "exist\t\t " sum "\t " count "\t " sum/count}'

ls *process_start*.log -1rt | tail -n 1 | xargs grep "getStat\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "getStat\t\t " sum "\t " count "\t " sum/count}'

# controller stats
echo -e "\ncontroller zk stats:"
echo -e "operation\t sum\t count\t avg"
echo -e "----------------------------------------"
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "asyncCreate\. path" | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "asyncCreate\t " sum "\t " count "\t " sum/count}'
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "asyncCreate\. path" | grep MESSAGES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  create MSG\t " sum "\t " count "\t " sum/count}'


ls *manager_start*.log -1rt | tail -n 1 | xargs grep "create\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "create\t\t " sum "\t " count "\t " sum/count}'

# read stats
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "read\t\t " sum "\t " count "\t " sum/count}'
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep CURRENTSTATES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  read CS\t " sum "\t " count "\t " sum/count}'
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep EXTERNALVIEW | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  read EV\t " sum "\t " count "\t " sum/count}'
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep CONFIGS | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  read CFG\t " sum "\t " count "\t " sum/count}'
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep LIVEINSTANCES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  read LI\t " sum "\t " count "\t " sum/count}'
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep CONTROLLER | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  read CTRL\t " sum "\t " count "\t " sum/count}'
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "zk-readData\." | grep MESSAGES | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "  read MSG\t " sum "\t " count "\t " sum/count}'

# update stats
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "update\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "update\t\t " sum "\t " count "\t " sum/count}'
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "update\." | grep EXTERNALVIEW | sed 's/.*time/time/g' | awk -F: '{sum+=$2; count++}END{print "  update EV\t " sum "\t " count "\t " sum/count}'
ls *manager_start*.log -1rt | tail -n 1 | xargs grep "update\." | grep CONTROLLER | sed 's/.*time/time/g' | awk -F: '{sum+=$2; count++}END{print "  update CTRL\t " sum "\t " count "\t " sum/count}'


ls *manager_start*.log -1rt | tail -n 1 | xargs grep "delete\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "delete\t\t " sum "\t "count "\t " sum/count}'


ls *manager_start*.log -1rt | tail -n 1 | xargs grep "getChildren\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "readChild\t " sum "\t " count "\t " sum/count}'


ls *manager_start*.log -1rt | tail -n 1 | xargs grep "exists\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "exist\t\t " sum "\t " count "\t " sum/count}'

ls *manager_start*.log -1rt | tail -n 1 | xargs grep "getStat\." | sed 's/.*time/time/g'|  awk -F: '{sum+=$2; count++}END{print "getStat\t\t " sum "\t " count "\t " sum/count}'

