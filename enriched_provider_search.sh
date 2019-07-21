#############################################################################################################
##                                         Provider Search                                                 ##     
#############################################################################################################
## This scripts calls the hive script to load Enriched Adobe Ah to Enriched Provider search table          ##
#############################################################################################################
## Date                          Modified By                         Comments                              ##
#############################################################################################################
## 02/16/2019                   Renuka Roy                      Initial Version                            ##
#############################################################################################################
#!/bin/ksh


source /u01/datascience/common/bin2/common.env 2>/dev/null
LogsDir=/u01/datascience/ab_digital/adobe_ah/logs 
log_file=enriched_provider_search-`date +%m%d%Y%H%M%S`.log

#prod_conformed_db=bi_ngx_dev_enc 
#adobe_db=adobe_enc  
#target_table=enriched_adobe_ah
 

echo "parameters-old are db=${1}, adobe=${2}, tgt=${3}, sid=${4}, src=${5}"  
prod_conformed_db=${1} 
adobe_db=${2} 
target_table=${3} 
sid=${4} 
source_conformed_db=${5}


echo "parameters are db=$prod_conformed_db, adobe=$adobe_db, tgt=$target_table, sid=$sid"
echo "parameters are db=$prod_conformed_db, adobe=$adobe_db, tgt=$target_table"  >> $LogsDir/$log_file


kinit -k -t ~/s018143.keytab S018143@AETH.AETNA.COM >> $LogsDir/$log_file 2>&1

  
#max_user_visit=`$BEELINE --outputformat=dsv --showHeader=false --hivevar hive.execution.engine=mr --hivevar prod_conformed_db=${prod_conformed_db} --hivevar target_table=${target_table} -e 'select count(1) from ${prod_conformed_db}.${target_table}'`


$BEELINE --outputformat=dsv --showHeader=false  --hivevar  param1=";;;;;" --hivevar param2=";;;;" --hivevar param3=";0"  --hivevar prod_conformed_db=${prod_conformed_db} --hivevar source_conformed_db=${source_conformed_db} --hivevar hive.execution.engine=tez --hivevar target_table=${target_table} --hivevar adobe_db=${adobe_db}  --hivevar row_created_by=${sid} -f /u01/datascience/ab_digital/adobe_ah/scripts/enriched_provider_search.hql >> $LogsDir/$log_file 2>&1



if [ $? -ne 0 ]
then
echo "Failed loading table Enriched Provider Search" >> $LogsDir/$log_file
sh ah_email_notification.sh rxroy@aetna.com Failure
exit 1
else 
echo "Table Enriched Provider Search AH loaded successfully." >> $LogsDir/$log_file
sh ah_email_notification.sh rxroy@aetna.com Success
fi

 
