#!/bin/ksh
echo "STARTING $(date)"
log_file=../logs/ah_nba_flu_funnel_detail.$(date +%Y%m%d%H%M%S).log
echo "Starting ah_nba_flu_funnel_detail.sh" > $log_file 

kinit -k -t ~/s018143.keytab S018143@AETH.AETNA.COM >> $log_file 2>&1



echo "Starting nba_flu_funnel.hql $(date)" >> $log_file 

/usr/bin/beeline -f ./nba_flu_funnel.hql --color=true -u 'jdbc:hive2://xhadhbasem1p.aetna.com:2181,xhadhivem1p.aetna.com:2181,xhadnmgrm1p.aetna.com:2181,xhadnnm1p.aetna.com:2181,xhadnnm2p.aetna.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-lb' >> $log_file 2>&1 

status=$?

echo "FINISHED nba_flu_funnel.hql Status: $status Date: $(date)" >> $log_file



if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "Starting nba_pcp_funnel.sql $(date)" >> $log_file 

/usr/bin/beeline -f ./nba_pcp_funnel.hql --color=true -u 'jdbc:hive2://xhadhbasem1p.aetna.com:2181,xhadhivem1p.aetna.com:2181,xhadnmgrm1p.aetna.com:2181,xhadnnm1p.aetna.com:2181,xhadnnm2p.aetna.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-lb' >> $log_file 2>&1 

status=$?

echo "FINISHED nba_pcp_funnel.hql Status: $status Date: $(date)" >> $log_file


if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "Starting nba_lab_er_funnel.sql $(date)" >> $log_file 

/usr/bin/beeline -f ./nba_lab_er_funnel.hql --color=true -u 'jdbc:hive2://xhadhbasem1p.aetna.com:2181,xhadhivem1p.aetna.com:2181,xhadnmgrm1p.aetna.com:2181,xhadnnm1p.aetna.com:2181,xhadnnm2p.aetna.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-lb' >> $log_file 2>&1 

status=$?

echo "FINISHED nba_lab_er_funnel.hql Status: $status Date: $(date)" >> $log_file



if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "Starting nba_bcs_funnel  $(date)" >> $log_file 

/usr/bin/beeline -f ./nba_bcs_funnel.hql --color=true -u 'jdbc:hive2://xhadhbasem1p.aetna.com:2181,xhadhivem1p.aetna.com:2181,xhadnmgrm1p.aetna.com:2181,xhadnnm1p.aetna.com:2181,xhadnnm2p.aetna.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-lb' >> $log_file 2>&1 

status=$?

echo "FINISHED nba_bcs_funnel  Status: $status Date: $(date)" >> $log_file



if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "Starting nba_diabetes_funnel  $(date)" >> $log_file 

/usr/bin/beeline -f ./nba_diabetes_funnel.hql --color=true -u 'jdbc:hive2://xhadhbasem1p.aetna.com:2181,xhadhivem1p.aetna.com:2181,xhadnmgrm1p.aetna.com:2181,xhadnnm1p.aetna.com:2181,xhadnnm2p.aetna.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-lb' >> $log_file 2>&1 

status=$?

echo "FINISHED nba_diabetes_funnel  Status: $status Date: $(date)" >> $log_file




if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "Starting nba_flu_outbreak_funnel  $(date)" >> $log_file 

/usr/bin/beeline -f ./nba_flu_outbreak_funnel.hql --color=true -u 'jdbc:hive2://xhadhbasem1p.aetna.com:2181,xhadhivem1p.aetna.com:2181,xhadnmgrm1p.aetna.com:2181,xhadnnm1p.aetna.com:2181,xhadnnm2p.aetna.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-lb' >> $log_file 2>&1 

status=$?

echo "FINISHED nba_flu_outbreak_funnel  Status: $status Date: $(date)" >> $log_file




if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "Starting merge_all_nba_funnels  $(date)" >> $log_file 

/usr/bin/beeline -f ./merge_all_nba_funnels.hql --color=true -u 'jdbc:hive2://xhadhbasem1p.aetna.com:2181,xhadhivem1p.aetna.com:2181,xhadnmgrm1p.aetna.com:2181,xhadnnm1p.aetna.com:2181,xhadnnm2p.aetna.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-lb' >> $log_file 2>&1 

status=$?
 


echo "FINISHED merge_funnels Status: $status Date: $(date)" >> $log_file



if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "Starting ah_nba_flu_funnel_detail_sqoop.sh $(date)" >> $log_file 

. ./ah_nba_flu_funnel_detail_sqoop.sh >> $log_file

status=$?

echo "FINISHED merge_funnels.sql Status: $status Date: $(date)" >> $log_file

if [ $status -gt 0 ]; then echo FAIL; return $status; fi

echo "FINISHED $(date)"
