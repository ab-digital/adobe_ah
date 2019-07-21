

------------------------------------------------------------------------------------------------------------
--                                Adobe Aetna Health Interaction                                        ----     
------------------------------------------------------------------------------------------------------------
-- This scripts builds a enriched view for NBA LAB Funnel reporting                                     ----
------------------------------------------------------------------------------------------------------------
-- Date                          Created By                         Comments                            ----
------------------------------------------------------------------------------------------------------------
-- 03/01/2019                    Renuka Roy                         Initial Version                     ----
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
--LOADING NBA LAB DATA
------------------------------------------------------------------------------------------------------------
-- NOTES: 
-- action id 7 is for unnecessary ER visits
-- action id's 15-19 are for non preferred labs
-- 
-- These allow us to show lack of presnce in Level 7; 
-- but we still don't see a 'good' event for level 7;
-- 
-- action 2 = UC visit
-- action 11-14 = preferred lab
-- dev_nba_enc.nba_campaign_x_action 
-- 
 

 
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


 
------------------------ Create Main NBA LAB Funnel Table -----------------------------------------------------



DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_201 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_201  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS 
SELECT   s.canvas_name
	   , s.external_user_id
       , count(*) as cnt
	   , min(from_unixtime(s.time)) as min_time
	   , max(from_unixtime(s.time)) as max_time
 FROM ${hivevar:braze_db}.pushnotifications_send as s
 LEFT JOIN ${hivevar:prod_conformed_db}.nba_exclude as e
 ON (e.proxy_id = s.external_user_id
     AND substr(from_unixtime(s.time), 1, 10) = e.event_date
     AND e.canvas_name = s.canvas_name)
where s.canvas_name in ('NBA: Non-Preferred Labs','NBA: Repeat ER')
  AND e.proxy_id is null
 group by s.external_user_id
       , s.canvas_name
;

--select * from bi_ngx_dev_enc.nba_tmp_201;


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_202 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_202   
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS 
select post_evar71
, case when user_agent like '%Mac OS X%' then 'iOS'
       when user_agent like '%Android%' then 'Android'
           when user_agent like '%Windows%' then 'Windows'
           else 'Other' end as OS
, post_evar139
, post_page_event_var2
, post_pagename
, min(substr(date_time,1,10)) as min_visit_date
, max(substr(date_time,1,10)) as max_visit_date
, count(*) as cnt_visit
from ${hivevar:adobe_db}.adobe_ngx
where post_evar139 <> ''
and post_evar139 in ('labs','urgent_care')
and ( post_page_event_var2 in (
      'AMACTION:aetnahealth:mobile:nba:article_view'
      ,'AMACTION:aetnahealth:mobile:nba:article_answer_select'
      ,'AMACTION:aetnahealth:mobile:search:article_card_carousel_view')
      or post_pagename in ( 
      'aetnahealth:mobile:nba:article_view'
	  ,'aetnahealth:mobile:search:article_card_carousel_view')
	 )
group by post_evar71
, case when user_agent like '%Mac OS X%' then 'iOS'
       when user_agent like '%Android%' then 'Android'
           when user_agent like '%Windows%' then 'Windows'
           else 'Other' end 
, post_evar139
, post_page_event_var2
, post_pagename
;

--select * from bi_ngx_dev_enc.nba_tmp_202 ;

 
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_203 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_203   
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS 
select  canvas_name 
	   , external_user_id as proxy_id
	   , min_time as min_visit_date
	   , max_time as max_visit_date
	   , cnt as cnt_visit
	   , 3 as level_nbr
	   , 'Campaign Triggered' as level_desc
	   from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_201
;

 
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_204 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_204   
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS 
select case post_evar139 when 'urgent_care' then 'NBA: Repeat ER'
                                 when 'labs' then 'NBA: Non-Preferred Labs'
			   end as canvas_name
	   , case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end as proxy_id
	   , min(min_visit_date) as min_visit_date
	   , max(max_visit_date) as max_visit_date
	   , sum(cnt_visit) as cnt_visit
	   , 4 as level_nbr
	   , 'Engagement View' as level_desc
	   from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_202
	   where post_page_event_var2 = 'AMACTION:aetnahealth:mobile:search:article_card_carousel_view'
          or post_pagename = 'aetnahealth:mobile:search:article_card_carousel_view'
	   group by case post_evar139 when 'urgent_care' then 'NBA: Repeat ER'
                                 when 'labs' then 'NBA: Non-Preferred Labs'
			   end 
	   , case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end
;


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_205 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_205    
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS 
select case post_evar139 when 'urgent_care' then 'NBA: Repeat ER'
                                 when 'labs' then 'NBA: Non-Preferred Labs'
			   end as canvas_name
	   , case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end as proxy_id
	   , min(min_visit_date) as min_visit_date
	   , max(max_visit_date) as max_visit_date
	   , sum(cnt_visit) as cnt_visit
	   , 5 as level_nbr
	   , 'Engagement Action' as level_desc
	   from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_202
	   where ( post_page_event_var2 in (
      'AMACTION:aetnahealth:mobile:nba:article_view'
      ,'AMACTION:aetnahealth:mobile:nba:article_answer_select')
      or post_pagename = 
      'aetnahealth:mobile:nba:article_view'
	          )
	   group by case post_evar139 when 'urgent_care' then 'NBA: Repeat ER'
                                 when 'labs' then 'NBA: Non-Preferred Labs'
			   end 
	   , case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end
;


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_206 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_206     
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS 
select case post_evar139 when 'urgent_care' then 'NBA: Repeat ER'
                                 when 'labs' then 'NBA: Non-Preferred Labs'
			   end as canvas_name
	   , case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end as proxy_id
	   , min(min_visit_date) as min_visit_date
	   , max(max_visit_date) as max_visit_date
	   , sum(cnt_visit) as cnt_visit
	   , 6 as level_nbr
	   , 'Engagement Success' as level_desc
	   from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_202
	   where  post_page_event_var2 = 'AMACTION:aetnahealth:mobile:nba:article_view'
           or post_pagename = 'aetnahealth:mobile:nba:article_view'
	   group by case post_evar139 when 'urgent_care' then 'NBA: Repeat ER'
                                 when 'labs' then 'NBA: Non-Preferred Labs'
			   end 
	   , case when trim(substr(post_evar71,1,3)) = '15~' then trim(substr(post_evar71,4)) else trim(post_evar71) end
;



 
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207 purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207      
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS 
select canvas_name
, proxy_id
, min_visit_date
, max_visit_date
, cnt_visit
, level_nbr
, level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_203 
;


insert into table ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207 
select canvas_name
, proxy_id
, min_visit_date
, max_visit_date
, cnt_visit
, level_nbr
, level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_204 as lvl4  
where lvl4.proxy_id in (select lvl3.proxy_id from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_203 as lvl3)
;


insert into table ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207 
select canvas_name
, lvl5.proxy_id
, min_visit_date
, max_visit_date
, cnt_visit
, level_nbr
, level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_205 as lvl5
where lvl5.proxy_id in (select lvl4.proxy_id from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207 as lvl4 where level_nbr = 4)
  ;


insert into table ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207 
select canvas_name
, lvl6.proxy_id
, min_visit_date
, max_visit_date
, cnt_visit
, level_nbr
, level_desc
, current_timestamp row_created_timestamp 
, 'S018143' row_created_by 
from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_206 as lvl6
where lvl6.proxy_id in (select lvl5.proxy_id from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207 as lvl5 where level_nbr = 5)
;


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_summary  purge;
CREATE  TABLE ${hivevar:prod_conformed_db}.nba_lab_funnel_summary     
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS 
select * from ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207 ;




--select * 
--from bi_ngx_dev_enc.nba_tmp_207
--;

--select r.canvas_name
--, r.level_nbr, r.level_desc
--, min(r.min_visit_date)  as min_visit_date
--, max(r.max_visit_date) as max_visit_date
--, sum(r.cnt_visit) as cnt_visit
--, count(distinct r.proxy_id) as cnt_memeber
--, current_timestamp row_created_timestamp 
--, 'S018143' row_created_by 
--from BI_NGX_PROD_ENC.nba_lab_funnel_detail_207 as R
--where r.proxy_id not in (select x.proxy_id from ${hivevar:prod_conformed_db}.aetna_digital_test_proxy_id as x)
--group by r.canvas_name
--, r.level_nbr, r.level_desc
--;



----------------------------------------PURGE TEMPORARY TABLES ---------------------------------------------------------------


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_201 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_202 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_203 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_204 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_205 purge;
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_206 purge; 
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.nba_lab_funnel_detail_207 purge; 

