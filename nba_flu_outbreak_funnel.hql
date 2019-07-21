------------------------------------------------------------------------------------------------------------
--                                Adobe Aetna Health Interaction                                        ----     
------------------------------------------------------------------------------------------------------------
-- This scripts builds a conformed view for NBA Flu_Outbreak Funnel reporting                           ----
------------------------------------------------------------------------------------------------------------
-- Date                          Created By                         Comments                            ----
------------------------------------------------------------------------------------------------------------
-- 06/29/2019                    Renuka Roy                         Initial Version                     ----
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
--LOADING NBA Flu_Outbreak DATA
------------------------------------------------------------------------------------------------------------

 
----Code for workload framework-----------------------------------------------------------------------------

--Input parameters For Dev
--set hivevar:prod_conformed_db=BI_NGX_DEV_ENC;
--set prod_conformed_db;  
--set hivevar:adobe_db=ADOBE_ENC;
--set source_db;  

--Input parameters For Prod  
set hivevar:prod_conformed_db=BI_NGX_PROD_ENC;
set prod_conformed_db;  
set hivevar:adobe_db=ADOBE_ENC;
set adobe_db; 
set hivevar:braze_db=BRAZE_ENC;
set braze_db; 
set tez.queue.name=prodconsumer;


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


------------------------ Create Main NBA Flu_Outbreak Funnel Table -----------------------------------------------------



DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail purge;

CREATE  TABLE ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail   
(campaign               string,
 cohort                 string,
 canvas                 string,
 proxy_id               string, 
 level_nbr              int,
 level_desc             string, 
 event_dt               date,
 event_desc_detail      string, 
 event_desc             string,
 event_cnt              int
)   ;




------------------------ Campaign Triggered LEVEL 3 PUSH Notification Send -----------------------------------------------------



insert into ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail 
select 
  'NBA: Flu Outbreak' as campaign
, 'NBA: Flu Outbreak - General' as  cohort 
, trim(canvas_name) as canvas
, external_user_id as proxy_id  
, 3 as level_nbr
, 'Campaign Triggered' as level_desc
, cast(substr(from_unixtime(time),1,10) as date) as event_dt
, 'Push' as event_desc_detail 
, 'Push' as event_desc
, 1
 FROM ${hivevar:braze_db}.pushnotifications_send as s
 where 
 lower(canvas_name)   like '%nba: flu outbreak%' 
 and canvas_name      not like '%[Testing%]%' 
;


------------------------ Engagement View (LEVEL 4)  PUSH Notification Open -----------------------------------------------------

 

insert into ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail 
select 
 'NBA: Flu Outbreak'           as campaign
,'NBA: Flu Outbreak - General' as  cohort 
, canvas_name as canvas
, external_user_id as proxy_id 
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(from_unixtime(time),1,10) as date) as event_dt
, 'Open' as event_desc_detail 
, 'Open' as event_desc
, 1
 FROM ${hivevar:braze_db}.pushnotifications_open as s
 where  
     lower(canvas_name)   like '%nba: flu outbreak%' 
 and canvas_name          not like '%[Testing%]%' 
 ;




 	
------------------------ Engagement View (LEVEL 4)  Braze custom Events -----------------------------------------------------
	

 
with push as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail where event_desc = 'Push' group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail 
select 
 'NBA: Flu Outbreak' as campaign
, push.cohort        as cohort
, push.canvas        as canvas 
, bec.external_user_id as proxy_id 
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, concat('Braze Events - ', bec.name) as event_desc_detail  
, substr(bec.name,instr(bec.name,'outbreak:')+9) event_desc 
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
, push 
where lower(bec.name) like '%nba_flu_outbreak%' and  
bec.name in 
( 
'aetnahealth:mobile:nba_flu_outbreak:no_thanks_select',
'aetnahealth:mobile:nba_flu_outbreak:map_view'
)
and push.proxy_id = bec.external_user_id
and cast(push.event_dt as string) <= substr(cast(from_unixtime(bec.time) as string),1,10)
and substr(cast(from_unixtime(bec.time) as string),1,10) < cast(date_add(push.event_dt, 60) as string)
;

 
 
with push as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail where event_desc = 'Push' group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail 
select 
 'NBA: Flu Outbreak' as campaign
, push.cohort        as cohort
, push.canvas        as canvas 
, bec.external_user_id as proxy_id 
, 5 as level_nbr
, 'Engagement Action' as level_desc
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, concat('Braze Events - ', 'find_provider_select') as event_desc_detail
, 'find_provider_select' as event_desc
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
, push 
where lower(bec.name) like '%nba_flu_outbreak%' and  
bec.name in 
(
'aetnahealth:mobile:nba_flu_outbreak:find_provider_select'  
) 
and push.proxy_id = bec.external_user_id
and cast(push.event_dt as string) <= substr(cast(from_unixtime(bec.time) as string),1,10)
and substr(cast(from_unixtime(bec.time) as string),1,10) < cast(date_add(push.event_dt, 60) as string)
;

 


------------------------ Engagement View (LEVEL 4)  Adobe NGX table postpagname -----------------------------------------------------
---So NBAs are just educational screens that prompt users to take action within the app usually to call a doctor to schedule an appt
   


with push as (select  canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail where event_desc = 'Push' group by canvas,proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail 
select   
 'NBA: Flu Outbreak' as campaign
, s.cohort        as cohort
, s.canvas        as canvas 
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_pagename as event_desc_detail
, 'map_view' as event_desc
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, push as s 
where an.post_pagename in 
('aetnahealth:mobile:nba_flu_outbreak:map_view'
) 
and lower(trim(post_evar167)) like '%flu_outbreak%' 
and lower(trim(post_evar168)) like '%flu_outbreak%' 
and trim(s.proxy_id) = 
( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end )  
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;

  


------------------------  Engagement Action (LEVEL 5)  Adobe NGX table Var2  -----------------------------------------------------
---So NBAs are just educational screens that prompt users to take action within the app usually to call a doctor to schedule an appt
   


with Push as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail where event_desc = 'Push' group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail 
select    
 'NBA: Flu Outbreak' as campaign
, s.cohort        as cohort
, s.canvas        as canvas 
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_page_event_var2 as event_desc_detail
, 'no_thanks_select' as event_desc
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, Push as s 
where an.post_page_event_var2 in 
(
'AMACTION:aetnahealth:mobile:nba_flu_outbreak:no_thanks_select'
) 
and lower(trim(post_evar167)) like '%flu_outbreak%' 
and lower(trim(post_evar168)) like '%flu_outbreak%' 
and trim(s.proxy_id) = 
( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end )   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;

  
  

with Push as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail where level_nbr=4  group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail 
select   
 'NBA: Flu Outbreak' as campaign
, s.cohort        as cohort
, s.canvas        as canvas 
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 5 as level_nbr
, 'Engagement Action' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_page_event_var2 as event_desc_detail 
, substr(an.post_page_event_var2,instr(an.post_page_event_var2,'outbreak:')+9) event_desc 
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, Push as s 
where an.post_page_event_var2 in 
(
 'AMACTION:aetnahealth:mobile:nba_flu_outbreak:find_provider_select'
) 
and lower(trim(post_evar167)) like '%flu_outbreak%' 
and lower(trim(post_evar168)) like '%flu_outbreak%' 
and trim(s.proxy_id) = 
( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end )  
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;




------------------------  Engagement Success (LEVEL 6)  Adobe NGX table Var2  -----------------------------------------------------
--the tags for search to 'complete' that NBA i.e. find and call a doctor dont contain nba_flu_outbreak as part of the tag

 

 
with Push as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail where level_nbr=5 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail 
select  
 'NBA: Flu Outbreak' as campaign
, s.cohort        as cohort
, s.canvas        as canvas 
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 6 as level_nbr
, 'Engagement Success' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_page_event_var2 as event_desc_detail 
,'App Call Provider' event_desc 
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, Push as s 
where an.post_page_event_var2 in  
('AMACTION:aetnahealth:mobile:provider_facility_detail:phone_number_select',
 'AMACTION:aetnahealth:mobile:search:phone_icon_select',
 'AMACTION:aetnahealth:mobile:search:result_provider_list_phone_icon_select'
) 
and trim(s.proxy_id) = 
( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end )   
and lower(trim(post_evar167)) like '%flu_outbreak%' 	
and lower(trim(post_evar168)) like '%flu_outbreak%' 
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;



------------------------  Behavior change (LEVEL 7)  NBA Flu_Outbreak Funnel Table DOES NOT EXIST YET -----------------------------------------------------

------------------------  FINAL STEP of creation of Flu_Outbreak summary table -----------------------------------------------------


DROP TABLE ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_summary ;

CREATE TABLE ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_summary 
AS 
Select 
  campaign
, cohort
, '' answer_card_number
, proxy_id
, min(event_dt) funnel_entry_dt 
, level_nbr
, level_desc 
, min(event_dt)  as level_min_dt  
, max(event_dt)  as level_max_dt 
, sum(event_cnt) as level_cnt 
, collect_set(event_desc) as lvl_3_event_desc_array
, current_timestamp row_created_timestamp
, 'S018143' row_created_by 
From ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail 
group by 
campaign
, cohort
, ''  
, proxy_id 
, level_nbr
, level_desc   
, current_timestamp 
, 'S018143' 
; 



----------------------------------------PURGE TEMPORARY TABLES ---------------------------------------------------------------




--DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_flu_outbreak_funnel_detail purge; 




