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
log_file=enriched_adobe_ah-`date +%m%d%Y%H%M%S`.log

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


showTable=`$BEELINE --outputformat=dsv --showHeader=false --hivevar hive.execution.engine=tez --hivevar prod_conformed_db=${prod_conformed_db} --hivevar target_table=${target_table} -e 'use ${prod_conformed_db}; show tables like "${target_table}";'`

#showTable=`$BEELINE --outputformat=dsv --showHeader=false  --hivevar prod_conformed_db=${1} --hivevar target_table=${9} -e 'use ${prod_conformed_db}; show tables like "${target_table}";'`


echo $showTable > $LogsDir/$log_file
if [ "$showTable" = "" ]
then 
          echo "The ${prod_conformed_db}.${target_table} table doesn't exist" >> $LogsDir/$log_file
       
else
          echo "The ${prod_conformed_db}.${target_table} table does exist,loading incremental data" >> $LogsDir/$log_file
  
max_user_visit=`$BEELINE --outputformat=dsv --showHeader=false --hivevar hive.execution.engine=mr --hivevar prod_conformed_db=${prod_conformed_db} --hivevar target_table=${target_table} -e 'select max(user_visit_datetime) from ${prod_conformed_db}.${target_table}'`

          year_max_user_visit=`echo $max_user_visit | cut -c1-4`
          month_max_user_visit=`echo $max_user_visit | cut -c6-7`
          day_max_user_visit=`echo $max_user_visit | cut -c9-10`
fi

#$BEELINE --outputformat=dsv --showHeader=false --hivevar year_max_user_visit=${year_max_user_visit} --hivevar month_max_user_visit=${month_max_user_visit} --hivevar day_max_user_visit=${day_max_user_visit}  --hivevar prod_conformed_db=${1} --hivevar source_table=${6} --hivevar target_stg_table=${7} --hivevar adobe_db=${4} --hivevar psbor_db=${8} --hivevar prod_stg_db=${10} --hivevar target_table=${9} -f ${11} >> $LogsDir/$log_file

$BEELINE --outputformat=dsv --showHeader=false --hivevar year_max_user_visit=${year_max_user_visit} --hivevar month_max_user_visit=${month_max_user_visit} --hivevar day_max_user_visit=${day_max_user_visit}   --hivevar prod_conformed_db=${prod_conformed_db} --hivevar source_conformed_db=${source_conformed_db} --hivevar hive.execution.engine=mr --hivevar target_table=${target_table} --hivevar adobe_db=${adobe_db}  --hivevar row_created_by=${sid} -f /u01/datascience/ab_digital/adobe_ah/scripts/enriched_adobe_ah.hql >> $LogsDir/$log_file



if [ $? -ne 0 ]
then
echo "Failed loading table Enriched Adobe AH" >> $LogsDir/$log_file
exit 1
else 
echo "Table Enriched Adobe AH loaded successfully." >> $LogsDir/$log_file
fi

 
