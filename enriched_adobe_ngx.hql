------------------------------------------------------------------------------------------------------------
--                                        Adobe Ngx                                                     ----     
------------------------------------------------------------------------------------------------------------
-- This scripts builds an enriched view for adobe ngx Aetna Health data                                 ----
------------------------------------------------------------------------------------------------------------
-- Date                          Modified By                         Comments                           ----
------------------------------------------------------------------------------------------------------------
-- 01/17/2019                      Renuka Roy                      Initial Version                      ----
------------------------------------------------------------------------------------------------------------


----Code for workload framework-------------------------------------------------------------------------------

--Input parameters For Prod
--set hivevar:prod_conformed_db=BI_NGX_DEV_ENC;
--set prod_conformed_db;   
--set hivevar:adobe_db=ADOBE_ENC;
--set source_db;  
--set tez.queue.name=prodconsumer;

--Input parameters For Dev 
--set hivevar:prod_enriched_db=BI_NGX_PROD_ENC;
--set prod_enriched_db; 
--set hivevar:source_conformed_db=prod_insights_conformed_enc; 
--set source_conformed_db;  
--set hivevar:adobe_db=ADOBE_ENC;
--set adobe_db;  
 

-----hive properties-------

set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.dbclass=fs;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.ppd=true;
SET hive.cbo.enable=true;
SET hive.exec.parallel=true;
SET hive.execution.engine=tez;

----Create backup table----------------------------------------------------------------------


    
--drop table ${hivevar:prod_enriched_db}.enriched_adobe_ngx_bkp;

--create table ${hivevar:prod_enriched_db}.enriched_adobe_ngx_bkp  
--STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
--AS  
--SELECT * from ${hivevar:prod_enriched_db}.enriched_adobe_ngx  ; 




----Decryption of post_evar124 membership id----------------------------------------------------------------------
--Added UDF function for decryption  

  

!run /var/webeng/hadoop/voltage_udf/1.0.1/createudf.hql



drop table ${hivevar:prod_enriched_db}.cumb_id_encrypted_1;
create table ${hivevar:prod_enriched_db}.cumb_id_encrypted_1 
 AS
SELECT  post_evar45  
 FROM  ${hivevar:adobe_db}.adobe_ngx where trim(post_evar45) != '' and length(trim(post_evar45)) < 20 ;

--and trim(post_evar41)='aetnahealth:mobile:login:encrypt_member'; 



drop table ${hivevar:prod_enriched_db}.cumb_id_encrypted;
create table ${hivevar:prod_enriched_db}.cumb_id_encrypted 
AS
SELECT DISTINCT post_evar45  ,
 if (instr(post_evar45, '\\') = 0, post_evar45,  
                    concat(substr(post_evar45,1,instr(post_evar45, '\\')),substr(post_evar45,instr(post_evar45, '\\') + 2))  
                   ) clean_post_evar45  
FROM  ${hivevar:prod_enriched_db}.cumb_id_encrypted_1 ;



drop table ${hivevar:prod_enriched_db}.cumb_id_encrypted_2;
create table ${hivevar:prod_enriched_db}.cumb_id_encrypted_2  
AS 
SELECT * ,
if (length(clean_post_evar45)<12, concat(clean_post_evar45,' '), clean_post_evar45) after_space_clean_evar45,
if (length(clean_post_evar45)<12, concat(' ',clean_post_evar45), clean_post_evar45) before_space_clean_evar45, 
if (instr(clean_post_evar45, '\\\\') = 0, clean_post_evar45,  
                    concat(substr(clean_post_evar45,1,instr(clean_post_evar45, '\\\\')),substr(clean_post_evar45,instr(clean_post_evar45, '\\\\') + 2))  
                   ) clean_post_evar45_1   
 FROM bi_ngx_prod_enc.cumb_id_encrypted ;



drop table ${hivevar:prod_enriched_db}.cumb_id_decrypted ;
create table ${hivevar:prod_enriched_db}.cumb_id_decrypted 
AS
select post_evar45 ,clean_post_evar45,  clean_post_evar45_1 , 
accessdata(clean_post_evar45,'US7ASCII2')  decrypt_member_id ,
accessdata(after_space_clean_evar45,'US7ASCII2')  decrypt_member_id_after ,
accessdata(before_space_clean_evar45,'US7ASCII2')  decrypt_member_id_before  ,
accessdata(clean_post_evar45_1,'US7ASCII2')  decrypt_clean_post_evar45_1   
from ${hivevar:prod_enriched_db}.cumb_id_encrypted_2;




------Create Generic Enriched Adobe NGX layer---------------------------------------------------------------------
  
 

DROP  TABLE IF EXISTS ${hivevar:prod_enriched_db}.enriched_adobe_ngx  ;

CREATE  TABLE IF NOT EXISTS ${hivevar:prod_enriched_db}.enriched_adobe_ngx   
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
SELECT 
Case 
     when substr(b.decrypt_member_id,1,2) in ('41','42','81') then trim(b.decrypt_member_id)  
     when substr(b.decrypt_member_id_after,1,2)  in ('41','42','81') then trim(b.decrypt_member_id_after)  
     when substr(b.decrypt_member_id_before,1,2) in ('41','42','81') then trim(b.decrypt_member_id_before)   
     when substr(b.decrypt_clean_post_evar45_1,1,2) in ('41','42','81') then trim(b.decrypt_clean_post_evar45_1)   
     else null end as decrypted_postevar45  ,   
Case 
     when substr(b.decrypt_member_id,1,2) in ('41','42','81') then trim(substr(b.decrypt_member_id,4))  
     when substr(b.decrypt_member_id_after,1,2)  in ('41','42','81') then trim(substr(b.decrypt_member_id_after,4))  
     when substr(b.decrypt_member_id_before,1,2) in ('41','42','81') then trim(substr(b.decrypt_member_id_before,4)) 
     when substr(b.decrypt_clean_post_evar45_1,1,2) in ('41','42','81') then trim(substr(b.decrypt_clean_post_evar45_1,4))   
     else null end as individual_analytics_identifier, 
case 
     when substr(b.decrypt_member_id,1,2) = '41' OR substr(b.decrypt_member_id_after,1,2) = '41' OR substr(b.decrypt_member_id_before,1,2) = '41' OR substr(b.decrypt_clean_post_evar45_1,1,2) = '41' then 'MEA'   
     when substr(b.decrypt_member_id,1,2) = '42' OR substr(b.decrypt_member_id_after,1,2) = '42' OR substr(b.decrypt_member_id_before,1,2) = '42' OR substr(b.decrypt_clean_post_evar45_1,1,2) = '42' then 'HMO'   
     when substr(b.decrypt_member_id,1,2) = '81' OR substr(b.decrypt_member_id_after,1,2) = '81' OR substr(b.decrypt_member_id_before,1,2) = '81' OR substr(b.decrypt_clean_post_evar45_1,1,2) = '81' then 'CMA'   
     else null end as source,  
a.*
FROM  ${hivevar:adobe_db}.adobe_ngx a LEFT OUTER JOIN ${hivevar:prod_enriched_db}.cumb_id_decrypted  b ON a.post_evar45 = b.post_evar45;



DROP  TABLE IF EXISTS  ${hivevar:prod_enriched_db}.cumb_id_encrypted purge;

DROP  TABLE IF EXISTS  ${hivevar:prod_enriched_db}.cumb_id_decrypted ;

  
drop table IF EXISTS ${hivevar:prod_enriched_db}.cumb_id_encrypted_1 purge;
  
drop table IF EXISTS ${hivevar:prod_enriched_db}.cumb_id_encrypted_2 purge;

