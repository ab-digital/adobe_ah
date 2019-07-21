#############################################################################################################
##                                               Adobe NGX                                                 ##     
#############################################################################################################
## This scripts calls the hive script to load adobe ngx data to target table                               ##
#############################################################################################################
## Date                          Modified By                         Comments                              ##
#############################################################################################################
## 01/16/2019                   Renuka Roy                      Initial Version                            ##
#############################################################################################################
#!/bin/ksh


source /u01/datascience/common/bin2/common.env 2>/dev/null
LogsDir=/u01/datascience/ab_digital/adobe_ah/logs 
log_file=enriched_adobe_ngx-`date +%m%d%Y%H%M%S`.log

#prod_enriched_db=bi_ngx_dev_enc 
#adobe_db=adobe_enc  
#target_table=enriched_adobe_ah 
 

echo "parameters-old are db=${1}, adobe=${2}, tgt=${3}, sid=${4}, src=${5}"  
prod_enriched_db=${1} 
adobe_db=${2} 
target_table=${3} 
sid=${4} 
source_conformed_db=${5}


echo "parameters are db=$prod_enriched_db, adobe=$adobe_db, tgt=$target_table, sid=$sid"
echo "parameters are db=$prod_enriched_db, adobe=$adobe_db, tgt=$target_table"  >> $LogsDir/$log_file


kinit -k -t ~/s018143.keytab S018143@AETH.AETNA.COM >> $LogsDir/$log_file 2>&1


$BEELINE --outputformat=dsv --showHeader=false --hivevar prod_enriched_db=${prod_enriched_db} --hivevar source_conformed_db=${source_conformed_db} --hivevar hive.execution.engine=tez --hivevar target_table=${target_table} --hivevar adobe_db=${adobe_db}  --hivevar row_created_by=${sid} -f /u01/datascience/ab_digital/adobe_ah/scripts/enriched_adobe_ngx.hql >> $LogsDir/$log_file 2>&1


if [ $? -ne 0 ]
then
echo "Failed loading table Enriched Adobe NGX" >> $LogsDir/$log_file
exit 1
else 
echo "Table Enriched Adobe NGX loaded successfully." >> $LogsDir/$log_file
fi

 
