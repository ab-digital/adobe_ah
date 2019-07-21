------------------------------------------------------------------------------------------------------------
--                                Adobe Aetna Health Interaction                                        ----     
------------------------------------------------------------------------------------------------------------
-- This scripts builds a conformed view for NBA FLU Funnel reporting                                    ----
------------------------------------------------------------------------------------------------------------
-- Date                          Created By                         Comments                            ----
------------------------------------------------------------------------------------------------------------
-- 03/01/2019                    Renuka Roy                         Initial Version                     ----
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
--LOADING NBA FLU DATA
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


------------------------ Create Main NBA FLU Funnel Table -----------------------------------------------------

DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_flu_funnel_detail purge;

CREATE  TABLE ${hivevar:prod_conformed_db}.nba_flu_funnel_detail   
(campaign               string,
 cohort                 string,
 proxy_id               string,
 experiment_group_type  string,
 level_nbr              int,
 level_desc             string,
 event_dt               date,
 event_desc             string,
 event_cnt              int
)   ;

-- NBA_PCP_Funnel_Insert.sql ;
-- Insert into nba_flu_funnel_detail for PCP campaign ;

-- Why is EVENT_DT getting null?

-- 3 if push or Interstitial Screen View;
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
select 'NBA: Flu' as campaign
, canvas_name as cohort
, external_user_id as proxy_id
, 'T' as experiment_group_type
, 3 as level_nbr
, 'Campaign Triggered' as level_desc
, cast(substr(from_unixtime(time),1,10) as date) as event_dt
, 'Push' as event_desc
, 1
 FROM ${hivevar:braze_db}.pushnotifications_send as s
 where canvas_name like 'NBA: Flu%' 
 and lower(canvas_name) not like '%nba: flu outbreak%' 	
 and canvas_name not like '%[Testing%]%'
 and external_user_id not in (select t.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as t)
;

-- Include questionnaire_view records where a matching PUSH exists (will use the push cohort) ;
with push as (select proxy_id, cohort, min(event_dt) as event_dt from ${hivevar:prod_conformed_db}.nba_flu_funnel_detail group by proxy_id, cohort)
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
select 'NBA: Flu' as campaign
, push.cohort
, bec.external_user_id as proxy_id
, 'T' as experiment_group_type
, 3 as level_nbr
, 'Campaign Triggered' as level_desc
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, 'Questionnaire Screen View.wP' as event_desc
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
, push
where bec.name like 'aetnahealth:mobile:nba_flu%questionnaire_view'
and bec.external_user_id not in (select t.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as t)
and push.proxy_id = bec.external_user_id
and cast(push.event_dt as string) <= substr(cast(from_unixtime(bec.time) as string),1,10)
and substr(cast(from_unixtime(bec.time) as string),1,10) < cast(date_add(push.event_dt, 60) as string)
;

-- Include remaining questionnaire_view records ;
with push as (select proxy_id, cohort, event_dt from ${hivevar:prod_conformed_db}.nba_flu_funnel_detail group by proxy_id, cohort, event_dt)
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
select 'NBA: Flu' as campaign
, case when name like 'aetnahealth:mobile:nba_flu_personalization_copd:%' then 'NBA: Flu, COPD'
     when name like 'aetnahealth:mobile:nba_flu_personalization_chronic:%' then 'NBA: Flu, Chronic'
     when name like 'aetnahealth:mobile:nba_flu_personalization_kids:%' then 'NBA: Flu, Kids'
     when name like 'aetnahealth:mobile:nba_flu_self_attest:%' then 'NBA: Flu, Self-Attest'
     else name
end as cohort
, bec.external_user_id as proxy_id
, 'T' as experiment_group_type
, 3 as level_nbr
, 'Campaign Triggered' as level_desc
, cast(substr(from_unixtime(bec.time),1,10) as date) as event_dt
, 'Questionnaire Screen View.woP' as event_desc
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
left outer join push
on (push.proxy_id = bec.external_user_id
    and cast(push.event_dt as string) = substr(from_unixtime(bec.time),1,10) )
where bec.name like 'aetnahealth:mobile:nba_flu%questionnaire_view' and lower(name) not like '%nba_flu_outbreak%'   
  and bec.external_user_id not in (select t.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as t)
  and push.proxy_id is null
;


-- 4 if questionnaire_view or Open ;
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
select 'NBA: Flu' as campaign
, case when name like 'aetnahealth:mobile:nba_flu_personalization_copd:%' then 'NBA: Flu, COPD'
     when name like 'aetnahealth:mobile:nba_flu_personalization_chronic:%' then 'NBA: Flu, Chronic'
     when name like 'aetnahealth:mobile:nba_flu_personalization_kids:%' then 'NBA: Flu, Kids'
     when name like 'aetnahealth:mobile:nba_flu_self_attest:%' then 'NBA: Flu, Self-Attest'
     else name
end as cohort
, external_user_id as proxy_id
, 'T' as experiment_group_type
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(from_unixtime(time),1,10) as date) as event_dt
, 'Questionnaire Screen View' as event_desc
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
where name like 'aetnahealth:mobile:nba_flu%questionnaire_view' and lower(name) not like '%nba_flu_outbreak%'  
and external_user_id not in (select t.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as t)
;


insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
select 'NBA: Flu' as campaign
, canvas_name as cohort
, external_user_id as proxy_id
, 'T' as experiment_group_type
, 4 as level_nbr
, 'Engagement View' as level_desc
, cast(substr(from_unixtime(time),1,10) as date) as event_dt
, 'Open' as event_desc
, 1
 FROM ${hivevar:braze_db}.pushnotifications_open as s
 where canvas_name like 'NBA: Flu%' and lower(canvas_name) not like '%nba: flu outbreak%' 	
 and canvas_name not like '%[Testing%]%'
 and external_user_id not in (select t.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as t)
 ;


-- 5 if any screen except questionnaire_view ;
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
select 'NBA: Flu' as campaign
, case when name like '%nba_flu_personalization_copd:%' then 'NBA: Flu, COPD'
     when name like '%nba_flu_personalization_chronic:%' then 'NBA: Flu, Chronic'
     when name like '%nba_flu_personalization_kids:%' then 'NBA: Flu, Kids'
     when name like '%nba_flu_self_attest:%' then 'NBA: Flu, Self-Attest'
     else name
end as canvas_name
, external_user_id as proxy_id
, 'T' as experiment_group_type
, 5 as level_nbr
, 'Engagement Action' as level_desc
, cast(substr(from_unixtime(time),1,10) as date) as event_dt
, substr(name, locate(':', name, locate('nba_flu_',name)) + 1 ) as event_desc
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
where name like '%nba_flu%:%' and lower(name) not like '%nba_flu_outbreak%' 
and name not like '%questionnaire_view'
and external_user_id not in (select t.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as t)
;


-- 6 if Braze Call or In App Call ;
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
select 'NBA: Flu' as campaign
, case when name like 'aetnahealth:mobile:nba_flu_personalization_copd:%' then 'NBA: Flu, COPD'
     when name like 'aetnahealth:mobile:nba_flu_personalization_chronic:%' then 'NBA: Flu, Chronic'
     when name like 'aetnahealth:mobile:nba_flu_personalization_kids:%' then 'NBA: Flu, Kids'
     when name like 'aetnahealth:mobile:nba_flu_self_attest:%' then 'NBA: Flu, Self-Attest'
     else name
end as canvas_name
, external_user_id as proxy_id
, 'T' as experiment_group_type
, 6 as level_nbr
, 'Engagement Success' as level_desc
, cast(substr(from_unixtime(time),1,10) as date) as event_dt
, 'Braze Call Doctor Click' as event_desc
, 1
from ${hivevar:braze_db}.braze_event_custom as bec
where name like '%nba_flu%' and lower(name) not like '%nba_flu_outbreak%' 
and name like '%call_provider_select'
and external_user_id not in (select t.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as t)
;


insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
select distinct 'NBA: Flu' as campaign
, s.canvas_name
, op.proxy_id
, 'T' as experiment_group_type
, 6 as level_nbr
, 'Engagement Success' as level_desc
, cast(substr(an.date_time,1,10) as date) as event_dt
, 'In App Call Doctor Click' as event_desc
, 1 
from ${hivevar:adobe_db}.adobe_ngx as an 
, ${hivevar:prod_conformed_db}.overwrite_proxy as op 
, ${hivevar:braze_db}.pushnotifications_send as s 
where an.post_page_event_var2 in 
('AMACTION:aetnahealth:mobile:search:phone_icon_select'
,'AMACTION:aetnahealth:mobile:provider_facility_detail:phone_number_select'
,'AMACTION:aetnahealth:mobile:claims_detail:claims_detail_phone_icon_select'
,'AMACTION:aetnahealth:mobile:claims_list:claims_list_phone_icon_select')
 and op.device = concat(an.post_visid_high, '~', an.post_visid_low)
 and op.visit_num = an.visit_num
 and op.visit_page_num = an.visit_page_num
 and s.canvas_name like 'NBA: Flu%'
 and s.canvas_name not like '%[Testing%]%' 
 and lower(s.canvas_name) not like '%nba: flu outbreak%' 	 
 and s.external_user_id = op.proxy_id
 and substr(from_unixtime(s.time),1,10) <= substr(an.date_time,1,10) 
 and s.external_user_id not in (select t.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as t)
;

-- 7 (Flu shot; None present) zzzzzz;
with push as (
  select proxy_id, min(event_dt) as event_dt, min(cohort)  as cohort, min(experiment_group_type) as experiment_group_type
  from ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
  where level_nbr = 3 group by proxy_id)
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_detail 
select 'NBA: Primary Care Physician' as campaign
, push.cohort 
, ae.proxy_id
, push.experiment_group_type
, 7 as level_nbr
, 'Behavior Change Success' as level_desc
, ae.action_dt as event_dt
, al.action_name as event_desc
, 1
from prod_nba_enc.nba_action_events as ae
, push
, prod_nba_enc.nba_action_id_list as al
where push.proxy_id = ae.proxy_id
and ae.action_id in (99)
and ae.action_dt > push.event_dt
and al.action_id = ae.action_id
and push.proxy_id not in (select t.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as t)
;




-- funnel_entry shows when each proxy entered a funnel (level 3) ;
drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_entry_tmp ;
create table ${hivevar:prod_conformed_db}.nba_flu_funnel_entry_tmp as 
select campaign
, cohort
, experiment_group_type
, proxy_id
, event_dt
, lag(event_dt) over (partition by campaign, cohort, proxy_id order by event_dt) as prev_dt
from ${hivevar:prod_conformed_db}.nba_flu_funnel_detail
where level_nbr = 3
order by campaign
, cohort
, experiment_group_type
, proxy_id
, event_dt
;

drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_entry ;
create table ${hivevar:prod_conformed_db}.nba_flu_funnel_entry as
select campaign
, cohort
, experiment_group_type
, proxy_id
, event_dt as funnel_entry_dt
, prev_dt
, (unix_timestamp(event_dt) - unix_timestamp( prev_dt ) ) / 86400 as days_since
from ${hivevar:prod_conformed_db}.nba_flu_funnel_entry_tmp
where (unix_timestamp(event_dt) - unix_timestamp( prev_dt ) ) / 86400  > 60 or prev_dt is null
;


drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_entry_expire;
create table ${hivevar:prod_conformed_db}.nba_flu_funnel_entry_expire as
select campaign
, cohort
, experiment_group_type
, proxy_id
, funnel_entry_dt
, prev_dt
, days_since
, lead(funnel_entry_dt) over (partition by campaign, cohort, experiment_group_type, proxy_id order by funnel_entry_dt) as funnel_expire_dt
from ${hivevar:prod_conformed_db}.nba_flu_funnel_entry
;


drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_level_3 ;
create table ${hivevar:prod_conformed_db}.nba_flu_funnel_level_3 as 
select fe.campaign
, fe.cohort
, fe.experiment_group_type
, fe.proxy_id
, fe.funnel_entry_dt
, fe.prev_dt
, fe.funnel_expire_dt
, min(fd.event_dt) as min_lvl_3_dt
, max(fd.event_dt) as max_lvl_3_dt
, sum(event_cnt) as lvl_3_cnt
, collect_set(event_desc) as lvl_3_event_desc_array
from ${hivevar:prod_conformed_db}.nba_flu_funnel_entry_expire as fe
, ${hivevar:prod_conformed_db}.nba_flu_funnel_detail as fd
where fe.campaign = fd.campaign
and fe.cohort = fd.cohort
and fe.proxy_id = fd.proxy_id
and fe.experiment_group_type = fd.experiment_group_type
and fe.funnel_entry_dt <= fd.event_dt
and (fe.funnel_expire_dt > fd.event_dt or fe.funnel_expire_dt is null)
and fd.level_nbr = 3
group by fe.campaign
, fe.cohort
, fe.experiment_group_type
, fe.proxy_id
, fe.funnel_entry_dt
, fe.prev_dt
, fe.funnel_expire_dt
;


-- get level 4 in two steps. One where there is a match to lvl3, and ;
-- another where we add the lvl3 rows that had no lvl4 event ;
drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_level_4 ;
create table ${hivevar:prod_conformed_db}.nba_flu_funnel_level_4 as 
select L3.campaign
, L3.cohort
, L3.experiment_group_type
, L3.proxy_id
, L3.funnel_entry_dt
, L3.prev_dt
, L3.funnel_expire_dt
, L3.min_lvl_3_dt
, L3.max_lvl_3_dt
, L3.lvl_3_cnt
, L3.lvl_3_event_desc_array
, min(L4.event_dt) as min_lvl_4_dt
, max(L4.event_dt) as max_lvl_4_dt
, sum(L4.event_cnt) as lvl_4_cnt
, collect_set(L4.event_desc) as lvl_4_event_desc_array
from ${hivevar:prod_conformed_db}.nba_flu_funnel_level_3 as L3
, ${hivevar:prod_conformed_db}.nba_flu_funnel_detail as L4
where L3.proxy_id              = L4.proxy_id
and cast(L3.funnel_entry_dt as string) <= cast(L4.event_dt as string)
and cast(L4.event_dt as string) < coalesce(cast(L3.funnel_expire_dt as string),'9999-12-31')
and L4.level_nbr = 4
group by L3.campaign
, L3.cohort
, L3.experiment_group_type
, L3.proxy_id
, L3.funnel_entry_dt
, L3.prev_dt
, L3.funnel_expire_dt
, L3.min_lvl_3_dt
, L3.max_lvl_3_dt
, L3.lvl_3_cnt
, L3.lvl_3_event_desc_array
;

with L4 as (select campaign, cohort, experiment_group_type, proxy_id, funnel_entry_dt from ${hivevar:prod_conformed_db}.nba_flu_funnel_level_4)
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_level_4 
select L3.campaign
, L3.cohort
, L3.experiment_group_type
, L3.proxy_id
, L3.funnel_entry_dt
, L3.prev_dt
, L3.funnel_expire_dt
, L3.min_lvl_3_dt
, L3.max_lvl_3_dt
, L3.lvl_3_cnt
, L3.lvl_3_event_desc_array
, null as min_lvl_4_dt
, null as max_lvl_4_dt
, null as lvl_4_cnt
, array(null) as lvl_4_event_desc_array
from ${hivevar:prod_conformed_db}.nba_flu_funnel_level_3 as L3
left outer join L4
on (L3.proxy_id              = L4.proxy_id
and L3.experiment_group_type = L4.experiment_group_type 
and cast(L3.funnel_entry_dt as string) = cast(L4.funnel_entry_dt as string) )
where L4.proxy_id is null
;

drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_level_5 ;
create table ${hivevar:prod_conformed_db}.nba_flu_funnel_level_5 as 
select L.campaign
, L.cohort
, L.experiment_group_type
, L.proxy_id
, L.funnel_entry_dt
, L.prev_dt
, L.funnel_expire_dt
, L.min_lvl_3_dt
, L.max_lvl_3_dt
, L.lvl_3_cnt
, L.lvl_3_event_desc_array
, L.min_lvl_4_dt
, L.max_lvl_4_dt
, L.lvl_4_cnt
, L.lvl_4_event_desc_array
, min(N.event_dt) as min_lvl_5_dt
, max(N.event_dt) as max_lvl_5_dt
, sum(N.event_cnt) as lvl_5_cnt
, collect_set(N.event_desc) as lvl_5_event_desc_array
from ${hivevar:prod_conformed_db}.nba_flu_funnel_level_4 as L
, ${hivevar:prod_conformed_db}.nba_flu_funnel_detail as N
where L.proxy_id              =  N.proxy_id
and cast(L.funnel_entry_dt as string) <= cast(N.event_dt as string)
and cast(N.event_dt as string) < coalesce(cast(L.funnel_expire_dt as string),'9999-12-31')
and N.level_nbr = 5
group by L.campaign
, L.cohort
, L.experiment_group_type
, L.proxy_id
, L.funnel_entry_dt
, L.prev_dt
, L.funnel_expire_dt
, L.min_lvl_3_dt
, L.max_lvl_3_dt
, L.lvl_3_cnt
, L.lvl_3_event_desc_array
, L.min_lvl_4_dt
, L.max_lvl_4_dt
, L.lvl_4_cnt
, L.lvl_4_event_desc_array
;

with L as (select campaign, cohort, experiment_group_type, proxy_id, funnel_entry_dt from ${hivevar:prod_conformed_db}.nba_flu_funnel_level_5)
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_level_5 
select N.campaign
, N.cohort
, N.experiment_group_type
, N.proxy_id
, N.funnel_entry_dt
, N.prev_dt
, N.funnel_expire_dt
, N.min_lvl_3_dt
, N.max_lvl_3_dt
, N.lvl_3_cnt
, N.lvl_3_event_desc_array
, N.min_lvl_4_dt
, N.max_lvl_4_dt
, N.lvl_4_cnt
, N.lvl_4_event_desc_array
, null as min_lvl_5_dt
, null as max_lvl_5_dt
, null as lvl_5_cnt
, array(null) as lvl_5_event_desc_array
from ${hivevar:prod_conformed_db}.nba_flu_funnel_level_4 as N
left outer join L
on (N.proxy_id = L.proxy_id
    and cast(N.funnel_entry_dt as string) = cast(L.funnel_entry_dt as string) )
where L.proxy_id is null
;

-- select * from bi_ngx_dev_enc.nba_flu_funnel_level_5;


drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_level_6 ;
create table ${hivevar:prod_conformed_db}.nba_flu_funnel_level_6 as 
select L.campaign
, L.cohort
, L.experiment_group_type
, L.proxy_id
, L.funnel_entry_dt
, L.prev_dt
, L.funnel_expire_dt
, L.min_lvl_3_dt
, L.max_lvl_3_dt
, L.lvl_3_cnt
, L.lvl_3_event_desc_array
, L.min_lvl_4_dt
, L.max_lvl_4_dt
, L.lvl_4_cnt
, L.lvl_4_event_desc_array
, L.min_lvl_5_dt
, L.max_lvl_5_dt
, L.lvl_5_cnt
, L.lvl_5_event_desc_array
, min(N.event_dt) as min_lvl_6_dt
, max(N.event_dt) as max_lvl_6_dt
, sum(N.event_cnt) as lvl_6_cnt
, collect_set(N.event_desc) as lvl_6_event_desc_array
from ${hivevar:prod_conformed_db}.nba_flu_funnel_level_5 as L
, ${hivevar:prod_conformed_db}.nba_flu_funnel_detail as N
where L.proxy_id =  N.proxy_id
and cast(L.funnel_entry_dt as string) <= cast(N.event_dt as string)
and cast(N.event_dt as string) < coalesce(cast(L.funnel_expire_dt as string),'9999-12-31')
and N.level_nbr = 6
and L.lvl_5_cnt is not null
group by L.campaign
, L.cohort
, L.experiment_group_type
, L.proxy_id
, L.funnel_entry_dt
, L.prev_dt
, L.funnel_expire_dt
, L.min_lvl_3_dt
, L.max_lvl_3_dt
, L.lvl_3_cnt
, L.lvl_3_event_desc_array
, L.min_lvl_4_dt
, L.max_lvl_4_dt
, L.lvl_4_cnt
, L.lvl_4_event_desc_array
, L.min_lvl_5_dt
, L.max_lvl_5_dt
, L.lvl_5_cnt
, L.lvl_5_event_desc_array
;

with L as (select campaign, cohort, experiment_group_type, proxy_id, funnel_entry_dt from ${hivevar:prod_conformed_db}.nba_flu_funnel_level_6)
insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_level_6
select N.campaign
, N.cohort
, N.experiment_group_type
, N.proxy_id
, N.funnel_entry_dt
, N.prev_dt
, N.funnel_expire_dt
, N.min_lvl_3_dt
, N.max_lvl_3_dt
, N.lvl_3_cnt
, N.lvl_3_event_desc_array
, N.min_lvl_4_dt
, N.max_lvl_4_dt
, N.lvl_4_cnt
, N.lvl_4_event_desc_array
, N.min_lvl_5_dt
, N.max_lvl_5_dt
, N.lvl_5_cnt
, N.lvl_5_event_desc_array
, null as min_lvl_5_dt
, null as max_lvl_5_dt
, null as lvl_5_cnt
, array(null) as lvl_5_event_desc_array
from ${hivevar:prod_conformed_db}.nba_flu_funnel_level_5 as N
left outer join L
on (N.proxy_id = L.proxy_id
    and cast(N.funnel_entry_dt as string) = cast(L.funnel_entry_dt as string) )
where L.proxy_id is null
;

--select * from bi_ngx_dev_enc.nba_flu_funnel_level_6;


drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_publish_detail ;

create table ${hivevar:prod_conformed_db}.nba_flu_funnel_publish_detail as
select campaign
, cohort
, experiment_group_type
, proxy_id
, funnel_entry_dt
, 3 as level_nbr
, 'Campaign Triggered' as level_desc
, min_lvl_3_dt as level_min_dt
, max_lvl_3_dt as level_max_dt
, lvl_3_cnt as level_cnt
, lvl_3_event_desc_array as level_event_array
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from  ${hivevar:prod_conformed_db}.nba_flu_funnel_level_6
where lvl_3_cnt is not null
;

insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_publish_detail
select campaign
, cohort
, experiment_group_type
, proxy_id
, funnel_entry_dt
, 4 as level_nbr
, 'Engagement View' as level_desc
, min_lvl_4_dt as level_min_dt
, max_lvl_4_dt as level_max_dt
, lvl_4_cnt as level_cnt
, lvl_4_event_desc_array as level_event_array
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from  ${hivevar:prod_conformed_db}.nba_flu_funnel_level_6
where lvl_4_cnt is not null
;

insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_publish_detail
select campaign
, cohort
, experiment_group_type
, proxy_id
, funnel_entry_dt
, 5 as level_nbr
, 'Engagement Action' as level_desc
, min_lvl_5_dt as level_min_dt
, max_lvl_5_dt as level_max_dt
, lvl_5_cnt as level_cnt
, lvl_5_event_desc_array as level_event_array
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from  ${hivevar:prod_conformed_db}.nba_flu_funnel_level_6
where lvl_5_cnt is not null
;

insert into ${hivevar:prod_conformed_db}.nba_flu_funnel_publish_detail
select campaign
, cohort
, experiment_group_type
, proxy_id
, funnel_entry_dt
, 6 as level_nbr
, 'Engagement Success' as level_desc
, min_lvl_6_dt as level_min_dt
, max_lvl_6_dt as level_max_dt
, lvl_6_cnt as level_cnt
, lvl_6_event_desc_array as level_event_array
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from  ${hivevar:prod_conformed_db}.nba_flu_funnel_level_6
where lvl_6_cnt is not null
;


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_flu_funnel_summary purge; 
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_flu_funnel_summary    
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS
select * from ${hivevar:prod_conformed_db}.nba_flu_funnel_publish_detail ;


----------------------------------------PURGE TEMPORARY TABLES ---------------------------------------------------------------

DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_flu_funnel_detail purge;
drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_entry_tmp  purge;
drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_entry  purge;
drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_entry_expire  purge;
drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_level_3 purge;
drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_level_4 purge;
drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_level_5 purge ;
drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_level_6 purge ;

drop table if exists ${hivevar:prod_conformed_db}.nba_flu_funnel_publish_detail;


-- select campaign, cohort, level_nbr, count(*) as cnt, count(distinct proxy_id) as cnt_user
-- from bi_ngx_dev_enc.nba_flu_funnel_publish_detail
-- group by campaign, cohort, level_nbr;

