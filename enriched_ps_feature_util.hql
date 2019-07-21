------------------------------------------------------------------------------------------------------------
-- This  script builds enriched Plan sponsor Featured Utilization data mart                             ----
------------------------------------------------------------------------------------------------------------
-- Date                          Modified By                         Comments                           ----
------------------------------------------------------------------------------------------------------------
-- 01/17/2019                    Renuka Roy                      Initial Version                        ----
------------------------------------------------------------------------------------------------------------

  
----Code for workload framework-----------------------------------------------------------------------------

--Input parameters For Dev
--set hivevar:prod_enriched_db=BI_NGX_DEV_ENC;
--set prod_enriched_db;
--set hivevar:adobe_db=ADOBE_ENC;
--set source_db;
--set tez.queue.name=prodconsumer;

--Input parameters For Dev
--set hivevar:prod_enriched_db=BI_NGX_PROD_ENC;
--set prod_enriched_db;
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


 
drop table ${hivevar:prod_enriched_db}.enriched_ps_feature_utilization_bkp;

create table ${hivevar:prod_enriched_db}.enriched_ps_feature_utilization_bkp  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
SELECT * from ${hivevar:prod_enriched_db}.enriched_ps_feature_utilization ; 



----Create base table----------------------------------------------------------------------



drop table ${hivevar:prod_enriched_db}.get_all_feature_recs ;

create table ${hivevar:prod_enriched_db}.get_all_feature_recs 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
select * from ${hivevar:adobe_db}.adobe_ngx where  
trim(post_evar41) in 
('aetnahealth:web:contact_us:contact_us_screen_view', 
'aetnahealth:mobile:manage_landing:contact_us_select', 
'aetnahealth:web:homepage:id_card_modal_view', 
'aetnahealth:mobile:manage_landing:member_id_card_select',  
'aetnahealth:mobile:manage_landing:claims_select', 
'aetnahealth:web:basic_navigation:claims_select' ,  
'aetnahealth:web:basic_navigation:manage_select' ,   
'aetnahealth:web:basic_navigation:eob_list_select',  
'aetnahealth:mobile:search:search_query', 
'aetnahealth:web:search:search_query',  
'aetnahealth:web:search:result_provider_list_impression_view',
'aetnahealth:mobile:search:result_provider_list_impression_view',  
'aetnahealth:mobile:search:result_provider_list_teledoc_linkout_leave', 
'aetnahealth:web:search:result_provider_list_teledoc_linkout_leave', 
'aetnahealth:mobile:improve:hoa_get_started_select', 
'aetnahealth:mobile:hoa:hoa_resume_optional_select',
'aetnahealth:mobile:hoa:hoa_restart_select', 
'aetnahealth:mobile:hoa:question_screen_view', 
'aetnahealth:mobile:improve:improve_landing_hoa_resume_select', 
'aetnahealth:mobile:hoa:complete_select', 
'aetnahealth:mobile:digital_coaching:goal_commit_select', 
'aetnahealth:mobile:digital_coaching:daily_activity_complete_select', 
'aetnahealth:mobile:health_actions:action_list_screen_view', 
'aetnahealth:mobile:records:landing_screen_view',
'aetnahealth:mobile:search:result_select',
'aetnahealth:web:search:result_select',
'aetnahealth:mobile:provider_price_a_medical_service:provider_price_a_medical_service_select',
'aetnahealth:web:provider_price_flow:price_medical_service_select')
OR
trim(post_pagename) in 
('aetnahealth:web:contact_us:contact_us_screen_view', 
'aetnahealth:mobile:manage_landing:contact_us_select', 
'aetnahealth:web:homepage:id_card_modal_view', 
'aetnahealth:mobile:manage_landing:member_id_card_select',  
'aetnahealth:mobile:manage_landing:claims_select', 
'aetnahealth:web:basic_navigation:claims_select' ,  
'aetnahealth:web:basic_navigation:manage_select' ,   
'aetnahealth:web:basic_navigation:eob_list_select',  
'aetnahealth:mobile:search:search_query', 
'aetnahealth:web:search:search_query',  
'aetnahealth:web:search:result_provider_list_impression_view',
'aetnahealth:mobile:search:result_provider_list_impression_view',  
'aetnahealth:mobile:search:result_provider_list_teledoc_linkout_leave', 
'aetnahealth:web:search:result_provider_list_teledoc_linkout_leave', 
'aetnahealth:mobile:improve:hoa_get_started_select', 
'aetnahealth:mobile:hoa:hoa_resume_optional_select',
'aetnahealth:mobile:hoa:hoa_restart_select', 
'aetnahealth:mobile:hoa:question_screen_view', 
'aetnahealth:mobile:improve:improve_landing_hoa_resume_select', 
'aetnahealth:mobile:hoa:complete_select', 
'aetnahealth:mobile:digital_coaching:goal_commit_select', 
'aetnahealth:mobile:digital_coaching:daily_activity_complete_select', 
'aetnahealth:mobile:health_actions:action_list_screen_view', 
'aetnahealth:mobile:records:landing_screen_view',
'aetnahealth:mobile:search:result_select',
'aetnahealth:web:search:result_select',
'aetnahealth:mobile:provider_price_a_medical_service:provider_price_a_medical_service_select',
'aetnahealth:web:provider_price_flow:price_medical_service_select')
;



----Seperate out the records by search_result  ----------------------------------------------------------------------



drop table ${hivevar:prod_enriched_db}.ps_data_filtered_1;

create table ${hivevar:prod_enriched_db}.ps_data_filtered_1 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
SELECT a.* 
from ${hivevar:prod_enriched_db}.get_all_feature_recs a
where trim(post_evar41) in  
('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select') and trim(post_evar122) in ('specialty', 'Specialty', 'Facility', 'facility', 'procedure', 'Procedure','Practitioner','practitioner','Medication','medication');   



drop table ${hivevar:prod_enriched_db}.ps_data_filtered_2;

create table ${hivevar:prod_enriched_db}.ps_data_filtered_2
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS   
SELECT a.* 
from ${hivevar:prod_enriched_db}.get_all_feature_recs  a
where trim(post_evar41)  not in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select');




drop table ${hivevar:prod_enriched_db}.ps_data_filtered;

create table ${hivevar:prod_enriched_db}.ps_data_filtered 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
Select * from ${hivevar:prod_enriched_db}.ps_data_filtered_2   ;



Insert into ${hivevar:prod_enriched_db}.ps_data_filtered 
Select * from ${hivevar:prod_enriched_db}.ps_data_filtered_1  ;



----Filter columns we need from base table for feature utilization ----------------------------------------------------------------------




drop table ${hivevar:prod_enriched_db}.ps_list_feature_recs;

create table ${hivevar:prod_enriched_db}.ps_list_feature_recs 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
select date_time, post_evar71, post_evar41,post_pagename, post_page_event_var2, post_evar122, mcvisid  
, case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4))  
                when post_evar71 = 'No Proxy ID Available' then null 
                when post_evar71 = '' then null 
                else trim(post_evar71) end as proxy_id
, concat(post_visid_high, '~', post_visid_low
        , '~', visit_num, '~', visit_start_time_gmt) as session_id
, case when user_agent like '%Mac OS X%' then 'iOS'
    when user_agent like '%Android%' then 'Android'
    when user_agent like '%Windows%' then 'Windows'
    else 'Other' end as OS_choice
, case  
      when instr(concat(post_pagename,post_page_event_var2),'mobile') > 0 then 'mobile'  
      when post_mobileappid <> ''   then 'mobile'  
      when instr(concat(post_pagename,post_page_event_var2),'web') > 0 then 'web'
      else 'web' 
   end as mobile_web_u 
   from ${hivevar:prod_enriched_db}.ps_data_filtered ;
  



----Join with Visit and Org arrangement table in order to pull Plan sponsor specific data ----------------------------------------------------------------------



--Create temp visits table
--drop table ${hivevar:prod_enriched_db}.ps_data_visit_util;

--create table ${hivevar:prod_enriched_db}.ps_data_visit_util 
--STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
--AS  
--select * from 
--      (select   b.visit_date, b.session_id, b.proxy_id, b.mobile_web, 
--       b.ps_unique_id, b.pssrc_nm, b.pssrc_mktseg_cd, b.ps_name,  b.org_name, b.funding_category, b.funding_category_description, b.customer_group_identifier as control_number, 
--       b.segment_name, b.sub_segment_name, b.Login_Indicator, b.First_Login_Indicator, b.First_Web_Login_Indicator, b.First_Mobile_Login_Indicator ,
--       b.individual_id, b.individual_analytics_identifier , b.individual_analytics_id_source, b.individual_analytics_subscriber_identifier, b.relationship_to_subscriber_description ,
--      row_number() over (partition BY b.session_id, b.proxy_id ) rank1 
--       from ${hivevar:prod_enriched_db}.visit b ) em   
--where em.rank1=1 ;



drop table ${hivevar:prod_enriched_db}.ps_data_visit_util;

create table ${hivevar:prod_enriched_db}.ps_data_visit_util 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
select b.visit_date, b.session_id, b.proxy_id, b.mobile_web, 
       b.ps_unique_id, b.pssrc_nm, b.pssrc_mktseg_cd, b.ps_name,  b.org_name, b.funding_category, b.funding_category_description, b.customer_group_identifier as control_number, 
       b.segment_name, b.sub_segment_name, b.Login_Indicator, b.First_Login_Indicator, b.First_Web_Login_Indicator, b.First_Mobile_Login_Indicator ,
       b.individual_id, b.individual_analytics_identifier , b.individual_analytics_id_source, b.individual_analytics_subscriber_identifier, b.relationship_to_subscriber_description  
       from ${hivevar:prod_enriched_db}.visit b  ; 

  
 
 

--SELECT org_id, org_arrangement_id, org_name, customer_group_identifier  FROM prod_insights_conformed_enc.enriched_membership LIMIT 100; 
--SELECT * FROM p4p_enc.obor_org ;


----Create final feature utilization table ----------------------------------------------------------------------



drop table ${hivevar:prod_enriched_db}.enriched_ps_feature_util;

create table ${hivevar:prod_enriched_db}.enriched_ps_feature_util 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
select  
session_id,
to_date(date_time)  date_time,
proxy_id ,  
post_evar41 action_type_name,
post_pagename, 
os_choice , 
mobile_web_u mobile_web ,  
'PS PRIMARY FEATURE' Report_Name, 
CASE  
when (trim(post_evar41)  in ('aetnahealth:web:homepage:id_card_modal_view', 'aetnahealth:mobile:manage_landing:member_id_card_select') 
        OR
        trim(post_pagename)   in ('aetnahealth:web:homepage:id_card_modal_view', 'aetnahealth:mobile:manage_landing:member_id_card_select'))
then 'Homepage'
when (trim(post_evar41)       in ('aetnahealth:web:contact_us:contact_us_screen_view', 'aetnahealth:mobile:manage_landing:contact_us_select') 
      OR
     trim(post_pagename)      in ('aetnahealth:web:contact_us:contact_us_screen_view', 'aetnahealth:mobile:manage_landing:contact_us_select'))
then 'Homepage' 
when (trim(post_evar41)       in ('aetnahealth:mobile:manage_landing:claims_select', 'aetnahealth:web:basic_navigation:claims_select', 'aetnahealth:web:basic_navigation:manage_select')   
        OR        
        trim(post_pagename)   in ('aetnahealth:mobile:manage_landing:claims_select', 'aetnahealth:web:basic_navigation:claims_select', 'aetnahealth:web:basic_navigation:manage_select'))  
then 'Claims'   
when (trim(post_evar41)       in ('aetnahealth:web:basic_navigation:eob_list_select') 
      OR
      trim(post_pagename)     in ('aetnahealth:web:basic_navigation:eob_list_select')) 
then 'Claims' 
when (trim(post_evar41)       in ('aetnahealth:mobile:search:search_query', 'aetnahealth:web:search:search_query')
      OR
      trim(post_pagename)     in ('aetnahealth:mobile:search:search_query', 'aetnahealth:web:search:search_query'))
then 'Search'  
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select') and  trim(post_evar122)  in ('specialty', 'Specialty')       then 'Search' 
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select') and  trim(post_evar122)  in ('Facility', 'facility')         then 'Search' 
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select') and  trim(post_evar122)  in ('procedure', 'Procedure')       then 'Search'  
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select')  and  trim(post_evar122) in ('Practitioner','practitioner')  then 'Search' 
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select')  and  trim(post_evar122) in ('Medication','medication')      then 'Search'  
when (trim(post_evar41) in ('aetnahealth:web:search:result_provider_list_impression_view','aetnahealth:mobile:search:result_provider_list_impression_view')  
      OR
      trim(post_pagename)  in ('aetnahealth:web:search:result_provider_list_impression_view', 'aetnahealth:mobile:search:result_provider_list_impression_view'))  
then 'Search' 
when (trim(post_evar41)    in ('aetnahealth:mobile:search:result_provider_list_teledoc_linkout_leave', 'aetnahealth:web:search:result_provider_list_teledoc_linkout_leave') 
      OR 
      trim(post_pagename)  in ('aetnahealth:mobile:search:result_provider_list_teledoc_linkout_leave', 'aetnahealth:web:search:result_provider_list_teledoc_linkout_leave')) 
then 'Search' 
when (trim(post_evar41)    in ('aetnahealth:mobile:improve:hoa_get_started_select', 'aetnahealth:mobile:hoa:hoa_resume_optional_select', 'aetnahealth:mobile:hoa:hoa_restart_select', 
                'aetnahealth:mobile:hoa:question_screen_view', 'aetnahealth:mobile:improve:improve_landing_hoa_resume_select') 
      OR 
      trim(post_pagename)  in ('aetnahealth:mobile:improve:hoa_get_started_select', 'aetnahealth:mobile:hoa:hoa_resume_optional_select', 'aetnahealth:mobile:hoa:hoa_restart_select', 
                'aetnahealth:mobile:hoa:question_screen_view', 'aetnahealth:mobile:improve:improve_landing_hoa_resume_select'))
then 'Stay Healthy (AH Mobile Only)' 
when trim(post_evar41)   in ('aetnahealth:mobile:hoa:complete_select')                              then 'Stay Healthy (AH Mobile Only)' 
when trim(post_evar41)   in ('aetnahealth:mobile:digital_coaching:goal_commit_select')              then 'Stay Healthy (AH Mobile Only)'  
when trim(post_evar41)   in ('aetnahealth:mobile:digital_coaching:daily_activity_complete_select')  then 'Stay Healthy (AH Mobile Only)' 
when trim(post_pagename) in ('aetnahealth:mobile:health_actions:action_list_screen_view')           then 'Stay Healthy (AH Mobile Only)' 
when trim(post_pagename) in ('aetnahealth:mobile:records:landing_screen_view')                      then 'Stay Healthy (AH Mobile Only)'  
Else 'Not Available' end as Category, 
CASE  
when (trim(post_evar41)  in ('aetnahealth:web:homepage:id_card_modal_view', 'aetnahealth:mobile:manage_landing:member_id_card_select') 
        OR
        trim(post_pagename)   in ('aetnahealth:web:homepage:id_card_modal_view', 'aetnahealth:mobile:manage_landing:member_id_card_select'))
then 'ID Card'
when (trim(post_evar41)       in ('aetnahealth:web:contact_us:contact_us_screen_view', 'aetnahealth:mobile:manage_landing:contact_us_select') 
      OR
     trim(post_pagename)      in ('aetnahealth:web:contact_us:contact_us_screen_view', 'aetnahealth:mobile:manage_landing:contact_us_select'))
then 'Contact Us' 
when (trim(post_evar41)       in ('aetnahealth:mobile:manage_landing:claims_select', 'aetnahealth:web:basic_navigation:claims_select', 'aetnahealth:web:basic_navigation:manage_select')   
        OR        
        trim(post_pagename)   in ('aetnahealth:mobile:manage_landing:claims_select', 'aetnahealth:web:basic_navigation:claims_select', 'aetnahealth:web:basic_navigation:manage_select'))  
then 'Claims List'   
when (trim(post_evar41)       in ('aetnahealth:web:basic_navigation:eob_list_select') 
      OR
      trim(post_pagename)     in ('aetnahealth:web:basic_navigation:eob_list_select')) 
then 'EOBs' 
when (trim(post_evar41)       in ('aetnahealth:mobile:search:search_query', 'aetnahealth:web:search:search_query')
      OR
      trim(post_pagename)     in ('aetnahealth:mobile:search:search_query', 'aetnahealth:web:search:search_query'))
then 'Search'  
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select') and  trim(post_evar122)  in ('specialty', 'Specialty')       then 'Specialty' 
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select') and  trim(post_evar122)  in ('Facility', 'facility')         then 'Facility' 
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select') and  trim(post_evar122)  in ('procedure', 'Procedure')       then 'Procedure'  
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select')  and  trim(post_evar122) in ('Practitioner','practitioner')  then 'Practitioner' 
when trim(post_evar41)  in ('aetnahealth:mobile:search:result_select','aetnahealth:web:search:result_select')  and  trim(post_evar122) in ('Medication','medication')      then 'Medication'  
when (trim(post_evar41) in ('aetnahealth:web:search:result_provider_list_impression_view','aetnahealth:mobile:search:result_provider_list_impression_view')  
      OR
      trim(post_pagename)  in ('aetnahealth:web:search:result_provider_list_impression_view', 'aetnahealth:mobile:search:result_provider_list_impression_view'))  
then 'Provider List View'  
when (trim(post_evar41) in ('aetnahealth:mobile:provider_price_a_medical_service:provider_price_a_medical_service_select','aetnahealth:web:provider_price_flow:price_medical_service_select')  
      OR
      trim(post_pagename)  in ('aetnahealth:mobile:provider_price_a_medical_service:provider_price_a_medical_service_select', 'aetnahealth:web:provider_price_flow:price_medical_service_select'))  
then 'Price Procedure'  
when (trim(post_evar41)    in ('aetnahealth:mobile:search:result_provider_list_teledoc_linkout_leave', 'aetnahealth:web:search:result_provider_list_teledoc_linkout_leave') 
      OR 
      trim(post_pagename)  in ('aetnahealth:mobile:search:result_provider_list_teledoc_linkout_leave', 'aetnahealth:web:search:result_provider_list_teledoc_linkout_leave')) 
then 'Teladoc' 
when (trim(post_evar41)    in ('aetnahealth:mobile:improve:hoa_get_started_select', 'aetnahealth:mobile:hoa:hoa_resume_optional_select', 'aetnahealth:mobile:hoa:hoa_restart_select', 
                'aetnahealth:mobile:hoa:question_screen_view', 'aetnahealth:mobile:improve:improve_landing_hoa_resume_select') 
      OR 
      trim(post_pagename)  in ('aetnahealth:mobile:improve:hoa_get_started_select', 'aetnahealth:mobile:hoa:hoa_resume_optional_select', 'aetnahealth:mobile:hoa:hoa_restart_select', 
                'aetnahealth:mobile:hoa:question_screen_view', 'aetnahealth:mobile:improve:improve_landing_hoa_resume_select'))
then 'Started Health Assessment' 
when trim(post_evar41)   in ('aetnahealth:mobile:hoa:complete_select')                              then 'Completed Health Assessment' 
when trim(post_evar41)   in ('aetnahealth:mobile:digital_coaching:goal_commit_select')              then 'Health Goals Started'  
when trim(post_evar41)   in ('aetnahealth:mobile:digital_coaching:daily_activity_complete_select')  then 'Health Goals Completed' 
when trim(post_pagename) in ('aetnahealth:mobile:health_actions:action_list_screen_view')           then 'Viewed Health Action' 
when trim(post_pagename) in ('aetnahealth:mobile:records:landing_screen_view')                      then 'Viewed PHR'  
Else 'Not Available' end as line ,
current_timestamp      row_created_timestamp,
'${hivevar:row_created_by}'   row_created_by 
from ${hivevar:prod_enriched_db}.ps_list_feature_recs   ;



 

--Create summary  table
drop table ${hivevar:prod_enriched_db}.ps_feature_util_summary;

create table ${hivevar:prod_enriched_db}.ps_feature_util_summary  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
select DISTINCT visit_date, b.session_id, b.proxy_id, b.mobile_web, 
       b.ps_unique_id, b.ps_name, b.pssrc_mktseg_cd,   b.org_name, b.funding_category, b.funding_category_description, b.control_number, 
       b.segment_name, b.sub_segment_name, b.Login_Indicator, b.First_Login_Indicator, b.First_Web_Login_Indicator, b.First_Mobile_Login_Indicator ,
       b.individual_id, b.individual_analytics_identifier , b.individual_analytics_id_source, b.individual_analytics_subscriber_identifier, b.relationship_to_subscriber_description  , c.line 
       from ${hivevar:prod_enriched_db}.ps_data_visit_util  b   
left outer join ${hivevar:prod_enriched_db}.enriched_ps_feature_util c
      on trim(b.session_id)=trim(c.session_id) and trim(b.proxy_id)=trim(c.proxy_id) 
where trim(b.proxy_id) is not null
UNION
select DISTINCT visit_date, b.session_id, b.proxy_id, b.mobile_web, 
       b.ps_unique_id, b.ps_name, b.pssrc_mktseg_cd,   b.org_name, b.funding_category, b.funding_category_description, b.control_number, 
       b.segment_name, b.sub_segment_name, b.Login_Indicator, b.First_Login_Indicator, b.First_Web_Login_Indicator, b.First_Mobile_Login_Indicator ,
       b.individual_id, b.individual_analytics_identifier , b.individual_analytics_id_source, b.individual_analytics_subscriber_identifier, b.relationship_to_subscriber_description  , c.line 
       from ${hivevar:prod_enriched_db}.ps_data_visit_util  b   
left outer join ${hivevar:prod_enriched_db}.enriched_ps_feature_util c
      on trim(b.session_id)=trim(c.session_id)  
where trim(b.proxy_id) is null
;





-- create and fill the table for sqoop to netezza;
 
 

DROP TABLE IF EXISTS ${hivevar:prod_enriched_db}.enriched_ps_feature_utilization purge;
CREATE  TABLE ${hivevar:prod_enriched_db}.enriched_ps_feature_utilization       
(
session_id string,  
date_time timestamp, 
proxy_id string, 
--individual_id string, 
--individual_analytics_identifier string,  
--individual_analytics_subscriber_identifier string, 
--relationship_to_subscriber_description string,  
--os_choice string, 
mobile_web string, 
ps_unique_id decimal(16,0),
ps_name string, 
--pssrc_mktseg_cd string, 
segment_name string, 
sub_segment_name string, 
org_name string, 
customer_group_identifier string, 
funding_category string, 
funding_category_description string, 
--report_name string, 
--category string, 
Login_Indicator int, 
First_Login_Indicator int, 
First_Web_Login_Indicator int,
First_Mobile_Login_Indicator int,
line string,  
row_created_timestamp timestamp, 
row_created_by string  
)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile; 




--INSERT  DATA 
insert into ${hivevar:prod_enriched_db}.enriched_ps_feature_utilization  
select DISTINCT 
session_id  ,  
visit_date  , 
proxy_id    , 
--individual_id  , 
--individual_analytics_identifier  ,  
--individual_analytics_subscriber_identifier  , 
--relationship_to_subscriber_description  ,  
--os_choice  , 
mobile_web , 
ps_unique_id  , 
ps_name  , 
--pssrc_mktseg_cd  , 
segment_name  , 
sub_segment_name  , 
org_name  , 
control_number  , 
funding_category  , 
funding_category_description  , 
--report_name  , 
--category  , 
Login_Indicator, 
First_Login_Indicator,
First_Web_Login_Indicator,
First_Mobile_Login_Indicator , 
line , 
current_timestamp      row_created_timestamp,
'${hivevar:row_created_by}'    row_created_by 
from ${hivevar:prod_enriched_db}.ps_feature_util_summary ;


-- Insert records for Price Procedure --deprecated


--insert into ${hivevar:prod_enriched_db}.enriched_ps_feature_utilization  
--select 
--session_id  ,  
--visit_date   , 
--proxy_id    , 
--individual_id  , 
--individual_analytics_identifier  ,  
--individual_analytics_subscriber_identifier  , 
--relationship_to_subscriber_description  ,  
--os_choice  , 
--mobile_web , 
--ps_unique_id  , 
--ps_name  , 
--pssrc_mktseg_cd  , 
--segment_name  , 
--sub_segment_name  , 
--org_name  , 
--control_number  , 
--funding_category  , 
--funding_category_description  , 
--report_name  , 
--category  , 
--Login_Indicator, 
--First_Login_Indicator,
--First_Web_Login_Indicator,
--First_Mobile_Login_Indicator , 
--'Price Procedure' line , 
--current_timestamp      row_created_timestamp,
--'${hivevar:row_created_by}'    row_created_by 
--from ${hivevar:prod_enriched_db}.ps_feature_util_summary where line='Procedure';
 

 
 


----drop temporary tables ----------------------------------------------------------------------

 
drop table if exists ${hivevar:prod_enriched_db}.get_all_feature_recs PURGE ;
drop table if exists ${hivevar:prod_enriched_db}.ps_data_filtered_1 PURGE ;
drop table if exists ${hivevar:prod_enriched_db}.ps_data_filtered_2 PURGE ;
drop table if exists ${hivevar:prod_enriched_db}.ps_data_filtered PURGE ;
drop table if exists ${hivevar:prod_enriched_db}.ps_list_feature_recs PURGE ;
drop table if exists ${hivevar:prod_enriched_db}.ps_data_final PURGE ;
drop table if exists ${hivevar:prod_enriched_db}.ps_data_visit_util PURGE ;
drop table if exists ${hivevar:prod_enriched_db}.enriched_ps_feature_util PURGE;


