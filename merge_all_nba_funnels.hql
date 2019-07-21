
------------------------------------------------------------------------------------------------------------
--                                Adobe Aetna Health Interaction                                        ----     
------------------------------------------------------------------------------------------------------------
-- This scripts builds a Merged view for NBA FLU Funnel reporting                                       ----
------------------------------------------------------------------------------------------------------------
-- Date                          Created By                         Comments                            ----
------------------------------------------------------------------------------------------------------------
-- 03/01/2019                    Renuka Roy                         Initial Version                     ----
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
--MERGE NBA DATA
------------------------------------------------------------------------------------------------------------

 
----Code for workload framework-----------------------------------------------------------------------------

--Input parameters For Dev
--set hivevar:prod_conformed_db=BI_NGX_DEV_ENC;
--set prod_conformed_db;  
--set hivevar:adobe_db=ADOBE_ENC;
--set source_db;  
set tez.queue.name=prodconsumer;

--Input parameters For Prod  
set hivevar:prod_conformed_db=BI_NGX_PROD_ENC;
set prod_conformed_db;  
set hivevar:adobe_db=ADOBE_ENC;
set adobe_db; 
set hivevar:braze_db=BRAZE_ENC;
set braze_db; 
 

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


 
--drop table ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail_bkp;

--create table ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail_bkp  
--STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
--AS  
--SELECT * from ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail; 




------------------------ Create Main NBA FLU Funnel Table -----------------------------------------------------


-- create and fill the table for sqoop to netezza;
 



DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail      
(
        campaign                             string,
        cohort                               string,
        experiment_group_type                string,
        proxy_id                             string,
        funnel_entry_dt                      TIMESTAMP,
        level_nbr                            int,
        level_desc                           string,
        level_min_dt                         TIMESTAMP,
        level_max_dt                         TIMESTAMP,
        level_cnt                            int,
        level_event_array                    string,
        row_created_timestamp                TIMESTAMP,
        row_created_by                       string   
) 
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


--MERGE NBA FLU DATA
insert into ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail   
select campaign, cohort, experiment_group_type, proxy_id, funnel_entry_dt
, level_nbr, level_desc,  level_min_dt, level_max_dt, level_cnt
, concat_ws(',',level_event_array) as level_event_array, current_timestamp, 'S018143' 
from ${hivevar:prod_conformed_db}.nba_flu_funnel_summary ;

--MERGE NBA PCP DATA
insert into ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail  
select 'NBA: PCP', canvas_name, experiment_group_type, proxy_id, min_visit_date, level_nbr, level_desc, min_visit_date, max_visit_date, cnt_visit, '',current_timestamp, 'S018143' 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_summary ;

--MERGE NBA LAB DATA
insert into ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail  
select canvas_name, canvas_name, 'T', proxy_id,   min_visit_date, level_nbr, level_desc,  min_visit_date,  max_visit_date, cnt_visit, '' ,current_timestamp, 'S018143' 
from  ${hivevar:prod_conformed_db}.nba_lab_funnel_summary  ;
 
--MERGE NBA BCS DATA
insert into ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail   
select campaign  ,cohort    ,' '    ,proxy_id  ,funnel_entry_dt  ,level_nbr,level_desc,level_min_dt,level_max_dt,level_cnt,lvl_3_event_desc_array ,row_created_timestamp,row_created_by  
from ${hivevar:prod_conformed_db}.nba_bcs_funnel_summary ;


--MERGE NBA DIABETES DATA
insert into ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail   
select campaign  ,cohort    ,' '    ,proxy_id  ,funnel_entry_dt  ,level_nbr,level_desc,level_min_dt,level_max_dt,level_cnt,lvl_3_event_desc_array,row_created_timestamp,row_created_by  
from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_summary ;

 

--MERGE NBA FLU OUTBREAK DATA
insert into ${hivevar:prod_conformed_db}.AH_nba_flu_funnel_detail   
select campaign  ,cohort    ,' '    ,proxy_id  ,funnel_entry_dt  ,level_nbr,level_desc,level_min_dt,level_max_dt,level_cnt,lvl_3_event_desc_array,row_created_timestamp,row_created_by  
from ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_summary ;



drop table ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207 purge;
drop table ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_300 purge;

drop table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 purge; 
drop table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_308 purge;

drop table ${hivevar:prod_conformed_db}.nba_flu_funnel_publish_detail purge;







