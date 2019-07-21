#############################################################################################################
##                                        PS Registration                                                  ##     
#############################################################################################################
## This scripts calls the hive script to load Enriched Adobe Ah to Enriched PS PS Registration  table      ##
#############################################################################################################
## Date                          Modified By                         Comments                              ##
#############################################################################################################
## 03/18/2019                   Renuka Roy                      Initial Version                            ##
#############################################################################################################
#!/bin/ksh


source /u01/datascience/common/bin2/common.env 2>/dev/null
LogsDir=/u01/datascience/ab_digital/adobe_ah/logs 
log_file=enriched_ps_registration-`date +%m%d%Y%H%M%S`.log

#prod_enriched_db=bi_ngx_dev_enc 
#adobe_db=adobe_enc  
#target_table=enriched_adobe_ah
 

echo "parameters-old are db=${1}, adobe=${2}, sid=${3}, src=${4}"  
prod_enriched_db=${1} 
adobe_db=${2}  
sid=${3} 
source_conformed_db=${4}
dev_insights=${5}


echo "parameters are db=$prod_enriched_db, adobe=$adobe_db, sid=$sid, dev_insights=$dev_insights"
echo "parameters are db=$prod_enriched_db, adobe=$adobe_db, sid=$sid, dev_insights=$dev_insights"  >> $LogsDir/$log_file

kinit -k -t ~/s018143.keytab S018143@AETH.AETNA.COM >> $LogsDir/$log_file 2>&1

$BEELINE --outputformat=dsv --showHeader=false  --hivevar prod_enriched_db=${prod_enriched_db} --hivevar prod_conformed_db=${source_conformed_db} --hivevar hive.execution.engine=tez  --hivevar adobe_db=${adobe_db} --hivevar dev_insights=${dev_insights} --hivevar row_created_by=${sid} -f /u01/datascience/ab_digital/adobe_ah/scripts/enriched_ps_registration.hql >> $LogsDir/$log_file 2>&1 


if [ $? -ne 0 ]
then
echo "Failed loading table Enriched PS Registration" >> $LogsDir/$log_file
exit 1
else 
echo "Table Enriched PS Registration loaded successfully." >> $LogsDir/$log_file
fi

 
