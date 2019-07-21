#!/bin/ksh
echo "STARTING $(date)"
log_file=./log/merge_sqoop.$(date +%Y%m%d%H%M%S).log
echo "Starting merge_sqoop.sh" > $log_file 

kinit -k -t ~/s042924.keytab S042924@AETH.AETNA.COM >> $log_file 2>&1

echo "Starting merge_funnels.sql $(date)" >> $log_file 

/usr/bin/beeline -f ./merge_funnels.sql --color=true -u 'jdbc:hive2://xhadhbasem1p.aetna.com:2181,xhadhivem1p.aetna.com:2181,xhadnmgrm1p.aetna.com:2181,xhadnnm1p.aetna.com:2181,xhadnnm2p.aetna.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-lb' >> $log_file 2>&1 

status=$?

echo "FINISHED merge_funnels.sql Status: $status Date: $(date)" >> $log_file

if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "Starting ah_nba_flu_funnel_detail_sqoop.sh $(date)" >> $log_file 

. ./ah_nba_flu_funnel_detail_sqoop.sh >> $log_file

status=$?

echo "FINISHED merge_funnels.sql Status: $status Date: $(date)" >> $log_file

if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "ENDIGN $(date)"
