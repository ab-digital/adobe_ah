## ah_nba_flu_funnel_detail_sqoop.sh
#!/bin/ksh

ROOT_PATH='/u01/datascience'

ENV_FILE="$ROOT_PATH/common/bin/common.env"

echo "ENV_FILE : " $ENV_FILE 

source $ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi

log_file=../logs/ah_nba_flu_funnel_detail_sqoop.$(date +%Y%m%d%H%M%S).log

#active_table="dst.AH_nba_flu_funnel_detail_$(date +%Y%m%d%H%M)"
backup_table="dst.AH_nba_flu_funnel_detail_bkp"
active_table="dst.AH_nba_flu_funnel_detail"

echo "Starting ah_nba_flu_funnel_detail_sqoop.sh $(date)"
echo "Log: $log_file"
echo "Active Table: $active_table"
echo "Backup Table: $backup_table"

/var/webeng/hadoop/sqoop_hdp2/bin/sqoop eval --connect "jdbc:netezza://PNZAMZP:5480/DDSTP000;schema=DST;" --username s018143 --password $(grep s018143 ~/.netrc| cut -d" " -f4) --query "TRUNCATE TABLE dst.AH_nba_flu_funnel_detail_bkp;" > $log_file 2>&1 
RC=$?
if [ $RC == 0 ]
then
        echo -e "Successfully Executed truncate  backup $backup_table table script"
else
        echo -e "Failed to  Execute truncate backup  $backup_table table script"
              echo "RC : " $RC

	      ZEKE_RC=$RC
fi

/var/webeng/hadoop/sqoop_hdp2/bin/sqoop eval --connect "jdbc:netezza://PNZAMZP:5480/DDSTP000;schema=DST;" --username s018143 --password $(grep s018143 ~/.netrc| cut -d" " -f4) --query "INSERT INTO dst.AH_nba_flu_funnel_detail_bkp SELECT * from dst.AH_nba_flu_funnel_detail;" > $log_file 2>&1 
RC1=$?
if [ $RC1 == 0 ]
then
        echo -e "Successfully Executed insert into $backup_table table script"
else
        echo -e "Failed to  Executed insert into $backup_table table script"
        echo "RC1 : " $RC1
	      ZEKE_RC=$RC
fi

/var/webeng/hadoop/sqoop_hdp2/bin/sqoop eval --connect "jdbc:netezza://PNZAMZP:5480/DDSTP000;schema=DST;" --username s018143 --password $(grep s018143 ~/.netrc| cut -d" " -f4) --query "TRUNCATE TABLE dst.AH_nba_flu_funnel_detail;" > $log_file 2>&1 
RC2=$?
if [ $RC2 == 0 ]
then
        echo -e "Successfully Executed truncate $active_table table script"
else
        echo -e "Failed to Executed truncate $active_table table script"
        echo "RC2 : " $RC2

	      ZEKE_RC=$RC
fi

echo "TRUNCATE TABLE status: $?"
echo "TRUNCATE TABLE status: $?" >> $log_file

/var/webeng/hadoop/sqoop_hdp2/bin/sqoop export --connect "jdbc:netezza://PNZAMZP:5480/DDSTP000;schema=DST;" --username s018143 --password $(grep s018143 ~/.netrc| cut -d" " -f4) --export-dir /shared/bi/ngx/prod/ah_nba_flu_funnel_detail/*/ --direct --table AH_NBA_FLU_FUNNEL_DETAIL -m 15 --fields-terminated-by '\t' --input-null-string "\\\\N" --input-null-non-string "\\\\N" --verbose -- --ctrl-chars --max-errors 5   >> $log_file 2>&1 

RC3=$?
if [ $RC3 == 0 ]
then
        echo -e "Successfully Sqooped $active_table table script"
else
        echo -e "Failed to Sqoop $active_table table script"
        echo "RC3 : " $RC3

	      ZEKE_RC=$RC
fi

status=$?

echo "================== FINISHED ==================" >> $log_file
echo "SQOOP export Status: $status" >> $log_file

echo "SQOOP export Status: $status"
echo "End ah_nba_flu_funnel_detail_sqoop.sh $(date)"

