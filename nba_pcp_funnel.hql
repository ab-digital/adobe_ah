
------------------------------------------------------------------------------------------------------------
--                                Adobe Aetna Health Interaction                                        ----     
------------------------------------------------------------------------------------------------------------
-- This scripts builds a enriched view for NBA PCP Funnel reporting                                     ----
------------------------------------------------------------------------------------------------------------
-- Date                          Created By                         Comments                            ----
------------------------------------------------------------------------------------------------------------
-- 03/01/2019                    Renuka Roy                         Initial Version                     ----
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
--LOADING NBA PCP DATA
------------------------------------------------------------------------------------------------------------

 
----Code for workload framework-----------------------------------------------------------------------------

--Input parameters For Dev
--set hivevar:prod_conformed_db=BI_NGX_DEV_ENC;
--set prod_conformed_db;  
--set hivevar:adobe_db=ADOBE_ENC;
--set source_db;  
--set tez.queue.name=prodconsumer;

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


------------------------ Create Main NBA PCP Funnel Table -----------------------------------------------------




-- assign each proxy a cohort;
-- very few were assigned two, we are taking the first as correct;


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_300 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_300  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS 
select distinct first_value(canvas_name) over (partition by external_user_id order by time) as canvas_name
, external_user_id
, first_value(from_unixtime(time)) over (partition by external_user_id order by time) as date_time
 FROM ${hivevar:braze_db}.pushnotifications_send as s
where canvas_name in ('NBA: PCP - Cohort A','NBA: PCP - Cohort B','NBA: PCP - Cohort C');



-- PUSH -> level 3 Engagement View yyyy	 ;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301a purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301a  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS  
SELECT   s.canvas_name
       , 'Push' as event
       , s.external_user_id
       , 'T' as experiment_group_type
       , count(*) as cnt
       , min(from_unixtime(s.time)) as min_time
       , max(from_unixtime(s.time)) as max_time
 FROM ${hivevar:braze_db}.pushnotifications_send as s
  LEFT JOIN ${hivevar:prod_conformed_db}.nba_exclude as e
 ON (e.proxy_id = s.external_user_id
     AND substr(from_unixtime(s.time), 1, 10) = e.event_date
     AND e.canvas_name = s.canvas_name
     AND e.level = 3)
where s.canvas_name in ('NBA: PCP - Cohort A','NBA: PCP - Cohort B','NBA: PCP - Cohort C')
AND e.proxy_id is null
 group by s.external_user_id
       , s.canvas_name
;


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS  
 SELECT  b.canvas_name
       , b.event
       , b.external_user_id
       , b.experiment_group_type
       , b.cnt
       , b.min_time
       , b.max_time
 FROM ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301a as b 
 LEFT JOIN  
 bi_ngx_prod_enc.nba_funnel_pcp_spike a   
 ON 
 (trim(a.individual_id_proxy) = trim(b.external_user_id) and b.min_time between '2019-03-08' and '2019-03-16' )
 where  a.individual_id_proxy is null   ;

  


-- Hold out -> Level 3 xxxxx ;
insert into ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301   
select case cohort_id 
  when 1 then 'NBA: PCP - Cohort B'
  when 2 then 'NBA: PCP - Cohort A'
  when 3 then 'NBA: PCP - Cohort C'
  when 4 then 'NBA: PCP - Cohort C'
  else 'ERROR'
  end as canvas_name
, 'Hold out' as event
, L.individual_id_proxy as proxy_id
, experiment_group_type
, count(*) as cnt_visit
, min(partition_date) as min_visit_date
, max(partition_date) as max_visit_date
FROM prod_nba_enc.nba_recmndtn_result_history as L
WHERE campaign_id = '5.1'
AND experiment_group_type = 'H'
GROUP BY case cohort_id 
  when 1 then 'NBA: PCP - Cohort B'
  when 2 then 'NBA: PCP - Cohort A'
  when 3 then 'NBA: PCP - Cohort C'
  when 4 then 'NBA: PCP - Cohort C'
  else 'ERROR'
  end 
, L.individual_id_proxy
, experiment_group_type
;





-- select * from bi_ngx_dev_enc.nba_tmp_301;

-- Interstitial -> level 3 Engagement View, level 4 Engagement Action;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_302 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_302  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS  
Select 
case when name like 'aetnahealth:mobile:nba_pcp_a:%' then 'NBA: PCP - Cohort A'
     when name like 'aetnahealth:mobile:nba_pcp_b:%' then 'NBA: PCP - Cohort B'
     when name like 'aetnahealth:mobile:nba_pcp_c:%' then 'NBA: PCP - Cohort C'
     when name like 'aetnahealth:mobile:nba_pcp_feedback:%' then 'NBA: PCP - Feedback'
     else name
end as canvas_name
, 'Interstitial' as event
, external_user_id 
, count(*) as cnt
, min(from_unixtime(time)) as min_time
, max(from_unixtime(time)) as max_time
from ${hivevar:braze_db}.braze_event_custom as bec
where name like 'aetnahealth:mobile:nba_pcp%:interstitial_screen_view%'
group by case when name like 'aetnahealth:mobile:nba_pcp_a:%' then 'NBA: PCP - Cohort A'
     when name like 'aetnahealth:mobile:nba_pcp_b:%' then 'NBA: PCP - Cohort B'
     when name like 'aetnahealth:mobile:nba_pcp_c:%' then 'NBA: PCP - Cohort C'
     when name like 'aetnahealth:mobile:nba_pcp_feedback:%' then 'NBA: PCP - Feedback'
     else name
end 
, external_user_id 
;

-- select * from bi_ngx_dev_enc.nba_tmp_302 ;

-- Open -> level 4 Engagement Action;
 
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_303 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_303 
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS  
SELECT   canvas_name
       , 'Open' as event
       , s.external_user_id
       , count(*) as cnt
       , min(from_unixtime(s.time)) as min_time
       , max(from_unixtime(s.time)) as max_time
 FROM ${hivevar:braze_db}.pushnotifications_open as s
where canvas_name in ('NBA: PCP - Cohort A','NBA: PCP - Cohort B','NBA: PCP - Cohort C')
 group by s.external_user_id
       , canvas_name
;

DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_304 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_304  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS
select 
case when name like 'aetnahealth:mobile:nba_pcp_a:%' then 'NBA: PCP - Cohort A'
     when name like 'aetnahealth:mobile:nba_pcp_b:%' then 'NBA: PCP - Cohort B'
     when name like 'aetnahealth:mobile:nba_pcp_c:%' then 'NBA: PCP - Cohort C'
     when name like 'aetnahealth:mobile:nba_pcp_feedback:%' then 'NBA: PCP - Feedback'
     else name
end as canvas_name
, 'answer_select/plv_click' as event
, external_user_id 
, count(*) as cnt
, min(from_unixtime(time)) as min_time
, max(from_unixtime(time)) as max_time
from ${hivevar:braze_db}.braze_event_custom as bec
where name like 'aetnahealth:mobile:nba_pcp%'
and (name like '%plv_click' or name like '%answer_select')
group by case when name like 'aetnahealth:mobile:nba_pcp_a:%' then 'NBA: PCP - Cohort A'
     when name like 'aetnahealth:mobile:nba_pcp_b:%' then 'NBA: PCP - Cohort B'
     when name like 'aetnahealth:mobile:nba_pcp_c:%' then 'NBA: PCP - Cohort C'
     when name like 'aetnahealth:mobile:nba_pcp_feedback:%' then 'NBA: PCP - Feedback'
     else name
end 
, external_user_id 
;

 

DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_305 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_305   
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS
select 
case when name like 'aetnahealth:mobile:nba_pcp_a:%' then 'NBA: PCP - Cohort A'
     when name like 'aetnahealth:mobile:nba_pcp_b:%' then 'NBA: PCP - Cohort B'
     when name like 'aetnahealth:mobile:nba_pcp_c:%' then 'NBA: PCP - Cohort C'
     when name like 'aetnahealth:mobile:nba_pcp_feedback:%' then 'NBA: PCP - Feedback'
     else name
end as canvas_name
, 'PCP Call' as event
, external_user_id 
, count(*) as cnt
, min(from_unixtime(time)) as min_time
, max(from_unixtime(time)) as max_time
from ${hivevar:braze_db}.braze_event_custom as bec
where bec.name = 'aetnahealth:mobile:nba_pcp_a:call_doctor_click'
group by case when name like 'aetnahealth:mobile:nba_pcp_a:%' then 'NBA: PCP - Cohort A'
     when name like 'aetnahealth:mobile:nba_pcp_b:%' then 'NBA: PCP - Cohort B'
     when name like 'aetnahealth:mobile:nba_pcp_c:%' then 'NBA: PCP - Cohort C'
     when name like 'aetnahealth:mobile:nba_pcp_feedback:%' then 'NBA: PCP - Feedback'
     else name
end 
, external_user_id 
;


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_306 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_306    
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS
select c.canvas_name
, 'App Call' as event
, op.proxy_id
, c.experiment_group_type 
, count(*) as cnt
, min(an.date_time) as min_time
, max(an.date_time) as max_time
from ${hivevar:adobe_db}.adobe_ngx as an
, ${hivevar:prod_conformed_db}.overwrite_proxy as op
, ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 as c
where an.post_page_event_var2 in ('AMACTION:aetnahealth:mobile:search:phone_icon_select'
,'AMACTION:aetnahealth:mobile:provider_facility_detail:phone_number_select'
,'AMACTION:aetnahealth:mobile:claims_detail:claims_detail_phone_icon_select'
,'AMACTION:aetnahealth:mobile:claims_list:claims_list_phone_icon_select')
and c.external_user_id = op.proxy_id
and c.min_time <= an.date_time
and op.device = concat(an.post_visid_high, '~', an.post_visid_low)
and op.visit_num = an.visit_num
and op.visit_page_num = an.visit_page_num

group by c.canvas_name
, op.proxy_id
, c.experiment_group_type 
;



-- use 301 instead of 300, so we exclude the bad push yyyy;

DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_308 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_308    
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS
select push.canvas_name
, L7.proxy_id 
, push.experiment_group_type 
, min(action_dt) as min_visit_date
, max(action_dt) as max_visit_date
, count(*) as cnt_visit
, 7 as level_nbr
, 'Behavior Change Success' as level_desc
from prod_nba_enc.nba_action_events as L7
, ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 as push
where push.external_user_id = L7.proxy_id
and L7.action_id in (1,3)
and L7.action_dt > push.min_time
group by push.canvas_name
, L7.proxy_id
, push.experiment_group_type 
;

-- Add Level 7 for Hold out!;


-- L3: Push (301) yyyy;
 
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307   
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS
select canvas_name
, external_user_id as proxy_id
, experiment_group_type
, min_time as min_visit_date
, max_time as max_visit_date
, cnt as cnt_visit
, 3 as level_nbr
, 'Campaign Triggered' as level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 ;

-- L4: Open (303) yyyy;
insert into table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307  
select canvas_name
, external_user_id as proxy_id
, 'T' as experiment_group_type
, min_time as min_visit_date
, max_time as max_visit_date
, cnt as cnt_visit
, 4 as level_nbr
, 'Engagement View' as level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_303 as L4
where L4.external_user_id in (select L3.proxy_id from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 as L3)
;

-- L4: Interstitial (302) yyyy;
insert into table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307  
select push.canvas_name
, L4.external_user_id as proxy_id
, 'T' as experiment_group_type
, L4.min_time as min_visit_date
, L4.max_time as max_visit_date
, L4.cnt as cnt_visit
, 4 as level_nbr
, 'Engagement View' as level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_302 as L4
, ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 as push
where L4.external_user_id in (select L3.proxy_id from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 as L3 where L3.level_nbr = 3)
and push.external_user_id = L4.external_user_id
;

-- L5: plv/answer (304) yyyy;
insert into table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307  
select push.canvas_name
, L5.external_user_id as proxy_id
, 'T' as experiment_group_type
, L5.min_time as min_visit_date
, L5.max_time as max_visit_date
, L5.cnt as cnt_visit
, 5 as level_nbr
, 'Engagement Action' as level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_304 as L5
, ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 as push
where L5.external_user_id in (select L4.proxy_id from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 as L4 where L4.level_nbr = 4)
and push.external_user_id = L5.external_user_id
;

-- L5: PCP Call (305) yyyy;
insert into table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 
select push.canvas_name
, L5.external_user_id as proxy_id
, 'T' as experiment_group_type
, L5.min_time as min_visit_date
, L5.max_time as max_visit_date
, L5.cnt as cnt_visit
, 5 as level_nbr
, 'Engagement Action' as level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_305 as L5
, ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 as push
where L5.external_user_id in (select L4.proxy_id from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 as L4 where L4.level_nbr = 4)
and push.external_user_id = L5.external_user_id
;

-- L5: App Call (306) yyyy;
insert into table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307  
select push.canvas_name
, L5.proxy_id
, push.experiment_group_type
, L5.min_time as min_visit_date
, L5.max_time as max_visit_date
, L5.cnt as cnt_visit
, 5 as level_nbr
, 'Engagement Action' as level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_306 as L5
, ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 as push
where L5.proxy_id in (select L4.proxy_id from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 as L4 where L4.level_nbr = 4)
and push.external_user_id = L5.proxy_id 
and push.experiment_group_type = 'T'
;

-- L6: PCP Call (305) yyyy;
with L5 as (select proxy_id, min_visit_date from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 where level_nbr = 5)
insert into table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 
select push.canvas_name
, L6.external_user_id as proxy_id
, 'T' as experiment_group_type
, L6.min_time as min_visit_date
, L6.max_time as max_visit_date
, L6.cnt as cnt_visit
, 6 as level_nbr
, 'Engagement Success' as level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_305 as L6
, ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 as push
, L5
where L6.external_user_id = L5.proxy_id
and L6.max_time > L5.min_visit_date
and push.external_user_id = L6.external_user_id
;

-- L6: App Call (306) yyyy Removed requirement that level 5 exist for proxy_id;
insert into table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 
select push.canvas_name
, L6.proxy_id
, push.experiment_group_type
, L6.min_time as min_visit_date
, L6.max_time as max_visit_date
, L6.cnt as cnt_visit
, 6 as level_nbr
, 'Engagement Success' as level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_306 as L6
, ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 as push
where L6.max_time > push.min_time 
and push.external_user_id = L6.proxy_id
;

-- L7: Went to doctor  yyyy;
insert into table ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 
select canvas_name
, L7.proxy_id
, L7.experiment_group_type
, min(min_visit_date) as min_visit_date
, max(max_visit_date) as max_visit_date
, count(*) as cnt_visit
, 7 as level_nbr
, 'Behavior Change Success' as level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_308 as L7
group by canvas_name
, L7.proxy_id
, L7.experiment_group_type
;



-- Exclude incorrect 25k PCP pushes sent out to members in March, 2019 
--DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_summary_1 purge; 
--CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_summary_1     
--STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
--AS 
--Select 
--canvas_name  ,
--proxy_id  ,
--experiment_group_type  ,
--min_visit_date  ,
--max_visit_date  ,
--cnt_visit  ,
--level_nbr  ,
--level_desc  ,
--row_created_timestamp  ,
--row_created_by  
--from 
--bi_ngx_prod_enc.nba_pcp_funnel_detail_307 b  
--LEFT JOIN  
--bi_ngx_prod_enc.nba_funnel_pcp_spike a   
--ON 
--(trim(a.individual_id_proxy) = trim(b.proxy_id) and b.min_visit_date between '2019-03-08' and '2019-03-16' )
--where b.level_nbr=3 and a.individual_id_proxy is null   ;


--Insert into ${hivevar:prod_conformed_db}.nba_pcp_funnel_summary_1 
--Select
--canvas_name  ,
--proxy_id  ,
--experiment_group_type  ,
--min_visit_date  ,
--max_visit_date  ,
--cnt_visit  ,
--level_nbr  ,
--level_desc ,
--row_created_timestamp  ,
--row_created_by   
--from 
--bi_ngx_prod_enc.nba_pcp_funnel_detail_307 b  
--where b.level_nbr > 3   ;

 


-- Exclude incorrect 2856 PCP pushes sent out to members in 07-17-2019   
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_summary purge; 
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_pcp_funnel_summary    
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS
select * from ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307  where min_visit_date != '2019-07-17'  ; 



----------------------------------------PURGE TEMPORARY TABLES ---------------------------------------------------------------

DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_300 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_301 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_302 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_303 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_304 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_305 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_306 purge; 
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_detail_307 purge; 
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_pcp_funnel_summary_1 purge; 

 