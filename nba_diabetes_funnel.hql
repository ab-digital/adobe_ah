------------------------------------------------------------------------------------------------------------
--                                Adobe Aetna Health Interaction                                        ----     
------------------------------------------------------------------------------------------------------------
-- This scripts builds a conformed view for NBA DIABETES Funnel reporting                                    ----
------------------------------------------------------------------------------------------------------------
-- Date                          Created By                         Comments                            ----
------------------------------------------------------------------------------------------------------------
-- 06/01/2019                    Renuka Roy                         Initial Version                     ----
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
--LOADING NBA DIABETES DATA
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


------------------------ Create Main NBA DIABETES Funnel Table -----------------------------------------------------



DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail purge;

CREATE  TABLE ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail   
(campaign               string,
 cohort                 string,
 canvas                 string,
 proxy_id               string, 
 level_nbr              int,
 level_desc             string, 
 event_dt               date,
 event_desc_detail      string,
 event_desc             string,
 answer_card_number     string,
 event_cnt              int
)   ;




------------------------ Campaign Triggered LEVEL 3 PUSH Notification Send -----------------------------------------------------


 
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select 'NBA: DIABETES' as campaign
, case when trim(canvas_name) like '%NBA: Diabetes, Care Gap%'      then 'NBA: Diabetes - care_gap' 
       when trim(canvas_name) like '%NBA: Diabetes, No PCP Visit%'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort 
, trim(canvas_name) as canvas
, external_user_id as proxy_id  
, 3 as level_nbr
, 'Campaign Triggered' as level_desc
, cast(substr(from_unixtime(time),1,10) as date) as event_dt
, 'Push' as event_desc_detail 
, 'Push' as event_desc
, ' ' answer_card_number
, 1
 FROM ${hivevar:braze_db}.pushnotifications_send as s
 where  
     lower(canvas_name) like '%nba: diabetes%' and canvas_name    not like '%[Testing%]%' 
;


------------------------ Engagement View (LEVEL 4)  PUSH Notification Open -----------------------------------------------------

 


insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select 'NBA: DIABETES' as campaign
, case when trim(canvas_name) like '%NBA: Diabetes, Care Gap%'      then 'NBA: Diabetes - care_gap' 
       when trim(canvas_name) like '%NBA: Diabetes, No PCP Visit%'  then 'NBA: Diabetes - no_pcp_visit' 
       else 'NA' end as cohort 
, canvas_name as canvas
, external_user_id as proxy_id 
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(from_unixtime(time),1,10) as date) as event_dt
, 'Open' as event_desc_detail 
, 'Open' as event_desc
, ' ' answer_card_number
, 1
 FROM ${hivevar:braze_db}.pushnotifications_open as s
 where  
     lower(canvas_name) like '%nba: diabetes%' 
     and canvas_name    not like '%[Testing%]%'  
;


------------------------------------ 1 - no pcp visit, 2 - care gap --------------------------------------------------------------
------------------------------------ START COHORT 1 -------------------------------------------------------------------------------
------------------------ Engagement View (LEVEL 4 and 5)  Braze custom Events -----------------------------------------------------
	

 
with push as (select proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where event_desc = 'Push' group by proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select 'NBA: DIABETES' as campaign
, push.cohort
, name canvas 
, bec.external_user_id as proxy_id 
, 4 as level_nbr
, 'Engagement View' as level_desc 
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, concat('Braze Events - ',bec.name) as event_desc_detail 
, substr(bec.name,instr(bec.name,'diabetes:')+9) event_desc 
, ' ' answer_card_number 
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
, push
where bec.name in	
(
'aetnahealth:mobile:nba_diabetes:carousel_screen_view',
'aetnahealth:mobile:nba_diabetes:remind_me_later_select'
) 
and push.proxy_id = bec.external_user_id
and cast(push.event_dt as string) <= substr(cast(from_unixtime(bec.time) as string),1,10)
and substr(cast(from_unixtime(bec.time) as string),1,10) < cast(date_add(push.event_dt, 60) as string)
;
 

 
with push as (select proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where event_desc = 'Push' group by proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select 'NBA: DIABETES' as campaign
, push.cohort
, name canvas 
, bec.external_user_id as proxy_id 
, 5 as level_nbr
, 'Engagement Action' as level_desc 
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, concat('Braze Events - ',bec.name) as event_desc_detail 
, 'find_provider_select' as event_desc
, ' ' answer_card_number 
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
, push
where bec.name in	
('aetnahealth:mobile:nba_diabetes:find_provider_select')   
and push.proxy_id = bec.external_user_id
and cast(push.event_dt as string) <= substr(cast(from_unixtime(bec.time) as string),1,10)
and substr(cast(from_unixtime(bec.time) as string),1,10) < cast(date_add(push.event_dt, 60) as string)
;


--SELECT 
--instr(properties,'card_number'), 
--if(instr(properties,'card_number')+14 > 14, substr(properties,instr(properties,'card_number')+14,1) ,'NA' ) carousel_after, 
--if(instr(properties,'card_number')-4  > 0,substr(properties,instr(properties,'card_number')-4,1),'NA') carousel_before, 
--properties,  * FROM braze_enc.braze_event_custom where lower(name) like '%nba%diabetes%';


------------------------ Engagement View (LEVEL 4)  Adobe NGX table postpagname -----------------------------------------------------
---So NBAs are just educational screens that prompt users to take action within the app usually to call a doctor to schedule an appt
   


with send as (select  canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where event_desc = 'Push' group by canvas,proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select   'NBA: DIABETES' as campaign 
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt 
, an.post_pagename  as event_desc_detail   
, substr(an.post_pagename,instr(an.post_pagename,'diabetes:')+9) 
, post_evar138 answer_card_number
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_pagename  in 
('aetnahealth:mobile:nba_diabetes:carousel_screen_view', 
 'aetnahealth:mobile:nba_diabetes:find_provider_view',
 'aetnahealth:mobile:nba_diabetes:remind_me_later_select')  
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('no_pcp_visit')   
and trim(s.proxy_id) =  
( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;

 

 

with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=4 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select   'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
,  an.post_page_event_var2   as event_desc_detail 
, substr( an.post_page_event_var2  ,instr( an.post_page_event_var2  ,'diabetes:')+9) 
, post_evar138 answer_card_number
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_page_event_var2 in  
('AMACTION:aetnahealth:mobile:nba_diabetes:carousel_screen_view', 
 'AMACTION:aetnahealth:mobile:nba_diabetes:remind_me_later_select') 
and trim(s.proxy_id) =  
( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('no_pcp_visit')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;



------------------------  Engagement Action (LEVEL 5)  Adobe NGX table Var2  -----------------------------------------------------
---So NBAs are just educational screens that prompt users to take action within the app usually to call a doctor to schedule an appt
   
  

with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=4  group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select   'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 5 as level_nbr
, 'Engagement Action' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_page_event_var2 as event_desc_detail 
, substr( an.post_page_event_var2  ,instr( an.post_page_event_var2  ,'diabetes:')+9) 
, ' '  answer_card_number
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_page_event_var2 in  
('AMACTION:aetnahealth:mobile:nba_diabetes:find_provider_select')  
and trim(s.proxy_id) =  
( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('no_pcp_visit')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;



------------------------  Engagement Success (LEVEL 6)  Adobe NGX table Var2  -----------------------------------------------------
--the tags for search to 'complete' that NBA i.e. find and call a doctor dont contain nba_bcs as part of the tag

 

 
with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=5 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select  'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 6 as level_nbr
, 'Engagement Success' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_page_event_var2 as event_desc_detail 
, 'App Call Provider' event_desc
, ' '  answer_card_number 
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_page_event_var2 in  
('AMACTION:aetnahealth:mobile:search:result_provider_list_phone_icon_select',
 'AMACTION:aetnahealth:mobile:search:phone_icon_select',
 'AMACTION:aetnahealth:mobile:provider_facility_detail:phone_number_select')  
and trim(s.proxy_id) =  
( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('no_pcp_visit')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;


 
------------------------------------ END COHORT 1 -------------------------------------------------------------------------------


------------------------------------ 1 - no pcp visit, 2 - care gap --------------------------------------------------------------

------------------------------------ START COHORT 2 -------------------------------------------------------------------------------
------------------------ Engagement View (LEVEL 4,5 and 6)  Braze custom Events -----------------------------------------------------
	

 
with push as (select proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where event_desc = 'Push' group by proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select 'NBA: DIABETES' as campaign
, push.cohort
, name canvas 
, bec.external_user_id as proxy_id 
, 4 as level_nbr
, 'Engagement View' as level_desc 
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, concat('Braze Events - ',bec.name) as event_desc_detail 
, substr(bec.name,instr(bec.name,'diabetes:')+9) as event_desc  
, ' '  answer_card_number 
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
, push
where bec.name in	 
(
'aetnahealth:mobile:nba_diabetes:questionnaire_view',
'aetnahealth:mobile:nba_diabetes:remind_me_later_select') 
and push.proxy_id = bec.external_user_id
and cast(push.event_dt as string) <= substr(cast(from_unixtime(bec.time) as string),1,10)
and substr(cast(from_unixtime(bec.time) as string),1,10) < cast(date_add(push.event_dt, 60) as string)
;
 

 
with push as (select proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where event_desc = 'Push' group by proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select 'NBA: DIABETES' as campaign
, push.cohort
, name canvas 
, bec.external_user_id as proxy_id 
, 5 as level_nbr
, 'Engagement Action' as level_desc 
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, concat('Braze Events - ',bec.name) as event_desc_detail 
, substr(bec.name,instr(bec.name,'diabetes:')+9) as event_desc
, case when substr(properties,instr(properties,'answer')+9,15) = 'microalbumin_on' then 'microalbumin_only' 
     when substr(properties,instr(properties,'answer')+9,15) = 'foot_check_only' then 'foot_check_only' 
     when substr(properties,instr(properties,'answer')+9,15) = 'microalbumin_an' then 'microalbumin_and_foot_check'
     when substr(properties,instr(properties,'answer')+9,11) = 'i_dont_know' then 'i_dont_know'  
else ' ' end as answer_card_number  
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
, push 
where bec.name in	 
( 
'aetnahealth:mobile:nba_diabetes:answer_select', 
'aetnahealth:mobile:nba_diabetes:microalbumin_only_provider_view',
'aetnahealth:mobile:nba_diabetes:foot_only_provider_view', 
'aetnahealth:mobile:nba_diabetes:find_provider_select',
'aetnahealth:mobile:nba_diabetes:resources_view'
) 
and push.proxy_id = bec.external_user_id
and cast(push.event_dt as string) <= substr(cast(from_unixtime(bec.time) as string),1,10)
and substr(cast(from_unixtime(bec.time) as string),1,10) < cast(date_add(push.event_dt, 60) as string)
;



 
with push as (select proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where event_desc = 'Push' group by proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select 'NBA: DIABETES' as campaign
, push.cohort
, name canvas 
, bec.external_user_id as proxy_id 
, 5 as level_nbr
, 'Engagement Action' as level_desc 
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, concat('Braze Events - ',bec.name) as event_desc_detail 
, substr(bec.name,instr(bec.name,'nba:')+4) as event_desc
, case when substr(properties,instr(properties,'answer')+9,15) = 'microalbumin_on' then 'microalbumin_only' 
     when substr(properties,instr(properties,'answer')+9,15) = 'foot_check_only' then 'foot_check_only' 
     when substr(properties,instr(properties,'answer')+9,15) = 'microalbumin_an' then 'microalbumin_and_foot_check'
     when substr(properties,instr(properties,'answer')+9,11) = 'i_dont_know' then 'i_dont_know'  
else ' ' end as answer_card_number  
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
, push 
where bec.name in	 
( 
'aetnahealth:mobile:nba:article_view',
'aetnahealth:mobile:nba:article_card_carousel_view' 
) 
and push.proxy_id = bec.external_user_id
and cast(push.event_dt as string) <= substr(cast(from_unixtime(bec.time) as string),1,10)
and substr(cast(from_unixtime(bec.time) as string),1,10) < cast(date_add(push.event_dt, 60) as string)
;


 

with push as (select proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where event_desc = 'Push' group by proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select 'NBA: DIABETES' as campaign
, push.cohort
, name canvas 
, bec.external_user_id as proxy_id 
, 6 as level_nbr
, 'Engagement Success' as level_desc 
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, concat('Braze Events - ',bec.name) as event_desc_detail 
, substr(bec.name,instr(bec.name,'diabetes:')+9) as event_desc 
, ' '  answer_card_number 
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
, push
where bec.name in	 
( 
'aetnahealth:mobile:nba_diabetes:praise_view',
'aetnahealth:mobile:nba_diabetes:done_select', 
'aetnahealth:mobile:nba_diabetes:call_provider_select')  
and push.proxy_id = bec.external_user_id
and cast(push.event_dt as string) <= substr(cast(from_unixtime(bec.time) as string),1,10)
and substr(cast(from_unixtime(bec.time) as string),1,10) < cast(date_add(push.event_dt, 60) as string)
;

 




------------------------ Engagement View (LEVEL 4)  Adobe NGX table postpagname -----------------------------------------------------
---So NBAs are just educational screens that prompt users to take action within the app usually to call a doctor to schedule an appt
   


with send as (select  canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where event_desc = 'Push' group by canvas,proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select   'NBA: DIABETES' as campaign 
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt 
, an.post_pagename as event_desc_detail 
, substr(an.post_pagename,instr(an.post_pagename,'diabetes:')+9)  as event_desc   
, post_evar138 answer_card_number 
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_pagename  in 
('aetnahealth:mobile:nba_diabetes:questionnaire_view' ) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ( 'care_gap')   
and trim(s.proxy_id) = ( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;


with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=4 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select   'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_page_event_var2   as event_desc_detail 
, substr(an.post_page_event_var2,instr(an.post_page_event_var2,'diabetes:')+9)  as event_desc   
, post_evar140 answer_card_number 
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_page_event_var2 in  
( 
 'AMACTION:aetnahealth:mobile:nba_diabetes:remind_me_later_select')  
and trim(s.proxy_id) = ( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('care_gap')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;



------------------------  Engagement Action (LEVEL 5)  Adobe NGX table Var2  -----------------------------------------------------
---So NBAs are just educational screens that prompt users to take action within the app usually to call a doctor to schedule an appt
   
  

with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=4 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select   'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 5 as level_nbr
, 'Engagement Action' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
,  an.post_page_event_var2  as event_desc_detail   
, substr(an.post_page_event_var2,instr(an.post_page_event_var2,'diabetes:')+9)  as event_desc   
, post_evar140 answer_card_number 
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_page_event_var2 in  
('AMACTION:aetnahealth:mobile:nba_diabetes:answer_select' 
'AMACTION:aetnahealth:mobile:nba_diabetes:find_provider_select' 
)  
and trim(s.proxy_id) = ( case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('care_gap')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;


 

with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=4 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select   'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 5 as level_nbr
, 'Engagement Action' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_pagename  as event_desc_detail  
, substr(an.post_pagename,instr(an.post_pagename,'diabetes:')+9)  as event_desc   
, post_evar138 answer_card_number
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_pagename in  
( 
'aetnahealth:mobile:nba_diabetes:microalbumin_only_provider_view',
'aetnahealth:mobile:nba_diabetes:foot_only_provider_view' , 
'aetnahealth:mobile:nba_diabetes:resources_view' 
)  
and trim(s.proxy_id) = ( case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('care_gap')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;



 

with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=4 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select   'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 5 as level_nbr
, 'Engagement Action' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_pagename  as event_desc_detail  
, substr(an.post_pagename,instr(an.post_pagename,'nba:')+4)  as event_desc   
, post_evar138 answer_card_number
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_pagename in  
(  
'aetnahealth:mobile:nba:article_view',
'aetnahealth:mobile:nba:article_card_carousel_view' 
)  
and trim(s.proxy_id) = ( case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('care_gap')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;



------------------------  Engagement Success (LEVEL 6)  Adobe NGX table Var2  -----------------------------------------------------
--the tags for search to 'complete' that NBA i.e. find and call a doctor dont contain nba_bcs as part of the tag

 

 
with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=5 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select  'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 6 as level_nbr
, 'Engagement Success' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_pagename  as event_desc_detail  
, substr(an.post_pagename,instr(an.post_pagename,'diabetes:')+9)  as event_desc 
, post_evar138 answer_card_number
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_pagename in  
( 
'aetnahealth:mobile:nba_diabetes:praise_view')  
and trim(s.proxy_id) = ( case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('care_gap')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;


 
 
with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=5 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select  'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 6 as level_nbr
, 'Engagement Success' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_page_event_var2  as event_desc_detail  
, substr(an.post_page_event_var2,instr(an.post_page_event_var2,'diabetes:')+9)  as event_desc   
, post_evar140 answer_card_number 
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_page_event_var2 in  
( 
'AMACTION:aetnahealth:mobile:nba_diabetes:done_select', 
'AMACTION:aetnahealth:mobile:nba_diabetes:call_provider_select' 
)  
and trim(s.proxy_id) = ( case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('care_gap')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;



 
with send as (select canvas, proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail where level_nbr=5 group by canvas, proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
select  'NBA: DIABETES' as campaign
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as cohort  
, case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end as canvas  
, case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end as proxy_id
, 6 as level_nbr
, 'Engagement Success' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, an.post_page_event_var2  as event_desc_detail  
, 'App Call Provider' as event_desc   
, post_evar140 answer_card_number 
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an  
, send as s 
where an.post_page_event_var2 in  
( 
'AMACTION:aetnahealth:mobile:search:phone_icon_select',
'AMACTION:aetnahealth:mobile:search:result_provider_list_phone_icon_select',
'AMACTION:aetnahealth:mobile:provider_facility_detail:phone_number_select'
)  
and trim(s.proxy_id) = ( case when trim(substr(an.post_evar71,1,3)) = '15~' then trim(substr(an.post_evar71,4)) else trim(an.post_evar71) end ) 
and s.cohort=
( case when lower(trim(post_evar168)) = 'care_gap'      then 'NBA: Diabetes - care_gap' 
       when lower(trim(post_evar168)) = 'no_pcp_visit'  then 'NBA: Diabetes - no_pcp_visit'  
       else 'NA' end 
) 
and lower(trim(post_evar167)) = 'diabetes' 
and lower(trim(post_evar168)) in ('care_gap')   
and substr(s.event_dt,1,10) <= substr(an.date_time,1,10)  
;

------------------------------------ END COHORT 2 -------------------------------------------------------------------------------



------------------------  Behavior change (LEVEL 7)  NBA DIABETES Funnel Table DOES NOT EXIST YET -----------------------------------------------------

------------------------  Last step of creation of Summary table  -----------------------------------------------------


DROP TABLE ${hivevar:prod_conformed_db}.nba_diabetes_funnel_summary ;

CREATE TABLE ${hivevar:prod_conformed_db}.nba_diabetes_funnel_summary 
AS 
Select 
  campaign
, cohort 
, case when event_desc like '%aetnahealth:mobile:nba_diabetes:answer_select%'         then answer_card_number  
       when event_desc like '%aetnahealth:mobile:nba_diabetes:carousel_screen_view%'  then answer_card_number   
       when event_desc like '%aetnahealth:mobile:nba:article_card_carousel_view%'     then answer_card_number   
       else '' end as answer_card_number   
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
From ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail 
group by 
campaign
, cohort
, case when event_desc like '%aetnahealth:mobile:nba_diabetes:answer_select%'         then answer_card_number  
       when event_desc like '%aetnahealth:mobile:nba_diabetes:carousel_screen_view%'  then answer_card_number   
       when event_desc like '%aetnahealth:mobile:nba:article_card_carousel_view%'     then answer_card_number         else '' end
, proxy_id 
, level_nbr
, level_desc   
, current_timestamp 
, 'S018143' 
; 



----------------------------------------PURGE TEMPORARY TABLES ---------------------------------------------------------------




--DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_diabetes_funnel_detail purge; 




