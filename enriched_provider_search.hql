------------------------------------------------------------------------------------------------------------
-- This  script builds enriched provider dataset and an extract of an enriched provider search dataset  ----
------------------------------------------------------------------------------------------------------------
-- Date                          Modified By                         Comments                           ----
------------------------------------------------------------------------------------------------------------
-- 01/17/2019                    Renuka Roy                      Initial Version                        ----
------------------------------------------------------------------------------------------------------------


----Code for workload framework-----------------------------------------------------------------------------

--Input parameters For Dev
--set hivevar:prod_conformed_db=BI_NGX_DEV_ENC;
--set prod_conformed_db;
--set hivevar:adobe_db=ADOBE_ENC;
--set source_db;
set tez.queue.name=prodconsumer;

--Input parameters For Dev
set hivevar:prod_conformed_db=BI_NGX_PROD_ENC;
set prod_conformed_db;
set hivevar:adobe_db=ADOBE_ENC;
set adobe_db;

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

set tez.queue.name=prodconsumer;

----Create backup table----------------------------------------------------------------------


 
--drop table ${hivevar:prod_conformed_db}.enriched_provider_search_bkp;

--create table ${hivevar:prod_conformed_db}.enriched_provider_search_bkp  
--STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
--AS  
--SELECT * from ${hivevar:prod_conformed_db}.enriched_provider_search; 



----Create base table----------------------------------------------------------------------


drop table ${hivevar:prod_conformed_db}.product_get_recs ;

create table ${hivevar:prod_conformed_db}.product_get_recs 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
SELECT date_time             AS user_visit_datetime ,   
From_unixtime(Cast(visit_start_time_gmt  AS    INT)) AS visit_start_datetime_gmt, 
trim(post_evar71)                  AS ProxyId, 
Trim(post_evar42)                  AS actionType, 
Trim(post_evar41)                  AS actionTypeName,
Trim(post_evar12)                  AS page_name, 
post_pagename                      AS post_page_name, 
Trim(post_evar121)                 AS Provider_List_View_Type_ID,
Trim(post_evar122)                 AS Provider_List_View_Type,               
Trim(post_evar120)                 AS ngx_search_term,
Trim(post_evar122)                 AS ngx_search_result_category, 
post_product_list                  AS post_product_list  ,
post_mvvar1                        AS typeahead_Search_Results_List ,  
CASE when user_agent like '%Mac OS X%' then 'iOS' 
     when user_agent like '%Android%'  then 'Android' 
     when user_agent like '%Windows%'  then 'Windows' 
     else 'Other' end AS OS_choice  ,  
case when instr(concat(post_pagename,post_page_event_var2),'mobile') > 0 then 'mobile'  
     when instr(concat(post_pagename,post_page_event_var2),'web') > 0 then 'web'  
     when post_page_event_var1 <> '' then 'web'  
else 'Unknown'  
end  AS  mobile_web_identifier, 
post_mobileappid                  AS mobile_app_identifier,   
cast(visit_page_num as INT)       AS visit_page_number  
FROM ${hivevar:adobe_db}.adobe_ngx 
where trim(post_product_list)!='';

 


--drop table ${hivevar:prod_conformed_db}.product_get_recs ;

--create table ${hivevar:prod_conformed_db}.product_get_recs 
--STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
--AS  
--SELECT  user_visit_datetime ,   
--visit_start_datetime_gmt,  
--source_proxy_identifier      AS ProxyId, 
--pct_of_page_viewed           AS actionType, 
--generic_link_name            AS actionTypeName,
--page_name                    AS page_name, 
--post_page_name               AS post_page_name,
--ngx_search_result_identifier AS Provider_List_View_Type_ID,
--ngx_search_result_category   AS Provider_List_View_Type,               
--ngx_search_term,
--ngx_search_result_category,
--post_product_list   ,
--typeahead_Search_Results_List , 
--os_choice ,    
--mobile_web  mobile_web_identifier,  
--post_mobile_appid mobile_app_identifier,  
--cast(visit_page_number as INT)   visit_page_number 
--FROM ${hivevar:prod_conformed_db}.enriched_adobe_ah 
--where trim(post_product_list)!='';

 


----Filter records we need from base table for provider search variable ----------------------------------------------------------------------



drop table ${hivevar:prod_conformed_db}.product_list_recs;

create table ${hivevar:prod_conformed_db}.product_list_recs 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
SELECT  user_visit_datetime, visit_start_datetime_gmt ,visit_page_number  ,actionTypeName action_type,page_name,  typeahead_Search_Results_List , mobile_web_identifier,   mobile_app_identifier , 
substr(ProxyId,4) Proxy_identifier, 
case   
when (lower(trim(actionTypeName))   like '%aetnahealth:web:search:search_query%' ) then 'Ahead Search Query' 
when (lower(trim(actionTypeName))   like '%aetnahealth:web:search:result_select%') then 'Ahead Search Result' 
--when (lower(trim(actionTypeName)) like '%aetnahealth:web:search:search_query%'  and substr(trim(post_product_list),1,5) != '${hivevar:param1}') then 'Provider Search Query' 
when (lower(trim(actionTypeName))   like '%provider_list_result_select%'  and substr(trim(post_product_list),1,5) != '${hivevar:param1}') then 'Provider List SELECT'   
when (lower(trim(actionTypeName))   like '%result_provider_list_impression_view%' and substr(trim(post_product_list),1,5) != '${hivevar:param1}')  then 'Provider List Search' 
else lower(trim(actionTypeName)) 
end actionTypeName, 
b.name Provider_List_View_Type_description ,
Provider_List_View_Type_ID,
Provider_List_View_Type,               
post_page_name,   
ngx_search_term, 
ngx_search_result_category, 
post_product_list , os_choice  
FROM ${hivevar:prod_conformed_db}.product_get_recs 
LEFT  OUTER JOIN ${hivevar:prod_conformed_db}.clinical_specialties  b 
ON   trim(Provider_List_View_Type_ID) = b.id 
WHERE  
--ProxyId like '%ML98SKBBBPXY%' and 
trim(ngx_search_term) !=''  
and  
(
 trim(lower(actionTypeName))  like '%search_query%'  OR
 trim(lower(actionTypeName))  like '%result_select%' OR  
 (trim(lower(actionTypeName)) like '%provider_list_impression_view%' and substr(trim(post_product_list),1,5) != '${hivevar:param1}')  
)
 and lower(trim(actionTypeName)) not like '%explanation_of_terms%' and trim(lower(actionTypeName)) not like '%provider_list_result_select%'
UNION
SELECT  user_visit_datetime, visit_start_datetime_gmt, visit_page_number  ,actionTypeName action_type,page_name, typeahead_Search_Results_List ,  mobile_web_identifier,   mobile_app_identifier , 
substr(ProxyId,4) Proxy_identifier, 
case   
when (lower(trim(actionTypeName))   like '%aetnahealth:web:search:search_query%' ) then 'Ahead Search Query' 
when (lower(trim(actionTypeName))   like '%aetnahealth:web:search:result_select%') then 'Ahead Search Result' 
when (lower(trim(actionTypeName))   like '%provider_list_result_select%'  and substr(trim(post_product_list),1,5) != '${hivevar:param1}') then 'Provider List SELECT'   
when (lower(trim(actionTypeName))   like '%result_provider_list_impression_view%' and substr(trim(post_product_list),1,5) != '${hivevar:param1}')  then 'Provider List Search' 
else lower(trim(actionTypeName)) 
end actionTypeName, 
b.name Provider_List_View_Type_description ,
Provider_List_View_Type_ID,
Provider_List_View_Type,               
post_page_name,   
ngx_search_term, 
ngx_search_result_category, 
post_product_list  , os_choice  
FROM ${hivevar:prod_conformed_db}.product_get_recs 
LEFT  OUTER JOIN ${hivevar:prod_conformed_db}.clinical_specialties  b 
ON   trim(Provider_List_View_Type_ID) = b.id 
WHERE  
ProxyId like '%ML98SKBBBPXY%'  and 
trim(ngx_search_term) !=''  
and   
trim(lower(actionTypeName)) like '%provider_list_result_select%' and trim(lower(page_name))='aetnahealth:web:provider_facility_detail:result_detail_screen_view'
order by Proxy_identifier, user_visit_datetime, visit_page_number ; 


  

---- provider search variable breakout, splitting each provider_identifier into multiple records ----------------------------------------------------------------------



drop table ${hivevar:prod_conformed_db}.product_split_rec1 ;

create table ${hivevar:prod_conformed_db}.product_split_rec1 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
SELECT
*,
concat('0',substr(CONCTNS.splitted_cnctns[1],instr(substr(CONCTNS.splitted_cnctns[1],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[1],1),'${hivevar:param2}')-1)) prv1_id,
substr(CONCTNS.splitted_cnctns[1],instr(substr(CONCTNS.splitted_cnctns[1],2),'${hivevar:param2}')+20,148) prv1_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[2],instr(substr(CONCTNS.splitted_cnctns[2],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[2],1),'${hivevar:param2}')-1)) prv2_id,
substr(CONCTNS.splitted_cnctns[2],instr(substr(CONCTNS.splitted_cnctns[2],2),'${hivevar:param2}')+20,148) prv2_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[3],instr(substr(CONCTNS.splitted_cnctns[3],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[3],1),'${hivevar:param2}')-1)) prv3_id,
substr(CONCTNS.splitted_cnctns[3],instr(substr(CONCTNS.splitted_cnctns[3],2),'${hivevar:param2}')+20,148) prv3_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[4],instr(substr(CONCTNS.splitted_cnctns[4],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[4],1),'${hivevar:param2}')-1)) prv4_id,
substr(CONCTNS.splitted_cnctns[4],instr(substr(CONCTNS.splitted_cnctns[4],2),'${hivevar:param2}')+20,148) prv4_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[5],instr(substr(CONCTNS.splitted_cnctns[5],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[5],1),'${hivevar:param2}')-1)) prv5_id,
substr(CONCTNS.splitted_cnctns[5],instr(substr(CONCTNS.splitted_cnctns[5],2),'${hivevar:param2}')+20,148) prv5_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[6],instr(substr(CONCTNS.splitted_cnctns[6],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[6],1),'${hivevar:param2}')-1)) prv6_id,
substr(CONCTNS.splitted_cnctns[6],instr(substr(CONCTNS.splitted_cnctns[6],2),'${hivevar:param2}')+20,148) prv6_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[7],instr(substr(CONCTNS.splitted_cnctns[7],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[7],1),'${hivevar:param2}')-1)) prv7_id,
substr(CONCTNS.splitted_cnctns[7],instr(substr(CONCTNS.splitted_cnctns[7],2),'${hivevar:param2}')+20,148) prv7_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[8],instr(substr(CONCTNS.splitted_cnctns[8],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[8],1),'${hivevar:param2}')-1)) prv8_id,
substr(CONCTNS.splitted_cnctns[8],instr(substr(CONCTNS.splitted_cnctns[8],2),'${hivevar:param2}')+20,148) prv8_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[9],instr(substr(CONCTNS.splitted_cnctns[9],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[9],1),'${hivevar:param2}')-1)) prv9_id,
substr(CONCTNS.splitted_cnctns[9],instr(substr(CONCTNS.splitted_cnctns[9],2),'${hivevar:param2}')+20,148) prv9_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[10],instr(substr(CONCTNS.splitted_cnctns[10],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[10],1),'${hivevar:param2}')-1)) prv10_id,
substr(CONCTNS.splitted_cnctns[10],instr(substr(CONCTNS.splitted_cnctns[10],2),'${hivevar:param2}')+20,148) prv10_evar, 
concat('0',substr(CONCTNS.splitted_cnctns[11],instr(substr(CONCTNS.splitted_cnctns[11],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[11],1),'${hivevar:param2}')-1)) prv11_id,
substr(CONCTNS.splitted_cnctns[11],instr(substr(CONCTNS.splitted_cnctns[11],2),'${hivevar:param2}')+20,148) prv11_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[12],instr(substr(CONCTNS.splitted_cnctns[12],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[12],1),'${hivevar:param2}')-1)) prv12_id,
substr(CONCTNS.splitted_cnctns[12],instr(substr(CONCTNS.splitted_cnctns[12],2),'${hivevar:param2}')+20,148) prv12_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[13],instr(substr(CONCTNS.splitted_cnctns[13],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[13],1),'${hivevar:param2}')-1)) prv13_id,
substr(CONCTNS.splitted_cnctns[13],instr(substr(CONCTNS.splitted_cnctns[13],2),'${hivevar:param2}')+20,148) prv13_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[14],instr(substr(CONCTNS.splitted_cnctns[14],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[14],1),'${hivevar:param2}')-1)) prv14_id,
substr(CONCTNS.splitted_cnctns[14],instr(substr(CONCTNS.splitted_cnctns[14],2),'${hivevar:param2}')+20,148) prv14_evar,    
concat('0',substr(CONCTNS.splitted_cnctns[15],instr(substr(CONCTNS.splitted_cnctns[15],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[15],1),'${hivevar:param2}')-1)) prv15_id,
substr(CONCTNS.splitted_cnctns[15],instr(substr(CONCTNS.splitted_cnctns[15],2),'${hivevar:param2}')+20,148) prv15_evar,   
concat('0',substr(CONCTNS.splitted_cnctns[16],instr(substr(CONCTNS.splitted_cnctns[16],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[16],1),'${hivevar:param2}')-1)) prv16_id,
substr(CONCTNS.splitted_cnctns[16],instr(substr(CONCTNS.splitted_cnctns[16],2),'${hivevar:param2}')+20,148) prv16_evar,   
concat('0',substr(CONCTNS.splitted_cnctns[17],instr(substr(CONCTNS.splitted_cnctns[17],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[17],1),'${hivevar:param2}')-1)) prv17_id,
substr(CONCTNS.splitted_cnctns[17],instr(substr(CONCTNS.splitted_cnctns[17],2),'${hivevar:param2}')+20,148) prv17_evar,   
concat('0',substr(CONCTNS.splitted_cnctns[18],instr(substr(CONCTNS.splitted_cnctns[18],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[18],1),'${hivevar:param2}')-1)) prv18_id,
substr(CONCTNS.splitted_cnctns[18],instr(substr(CONCTNS.splitted_cnctns[18],2),'${hivevar:param2}')+20,148) prv18_evar,  
concat('0',substr(CONCTNS.splitted_cnctns[19],instr(substr(CONCTNS.splitted_cnctns[19],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[19],1),'${hivevar:param2}')-1)) prv19_id,
substr(CONCTNS.splitted_cnctns[19],instr(substr(CONCTNS.splitted_cnctns[19],2),'${hivevar:param2}')+20,148) prv19_evar,   
concat('0',substr(CONCTNS.splitted_cnctns[20],instr(substr(CONCTNS.splitted_cnctns[20],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[20],1),'${hivevar:param2}')-1)) prv20_id,
substr(CONCTNS.splitted_cnctns[20],instr(substr(CONCTNS.splitted_cnctns[20],2),'${hivevar:param2}')+20,148) prv20_evar,
concat('0',substr(CONCTNS.splitted_cnctns[21],instr(substr(CONCTNS.splitted_cnctns[21],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[21],1),'${hivevar:param2}')-1)) prv21_id,
substr(CONCTNS.splitted_cnctns[21],instr(substr(CONCTNS.splitted_cnctns[21],2),'${hivevar:param2}')+20,148) prv21_evar,
concat('0',substr(CONCTNS.splitted_cnctns[22],instr(substr(CONCTNS.splitted_cnctns[22],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[22],1),'${hivevar:param2}')-1)) prv22_id,
substr(CONCTNS.splitted_cnctns[22],instr(substr(CONCTNS.splitted_cnctns[22],2),'${hivevar:param2}')+20,148) prv22_evar,
concat('0',substr(CONCTNS.splitted_cnctns[23],instr(substr(CONCTNS.splitted_cnctns[23],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[23],1),'${hivevar:param2}')-1)) prv23_id,
substr(CONCTNS.splitted_cnctns[23],instr(substr(CONCTNS.splitted_cnctns[23],2),'${hivevar:param2}')+20,148) prv23_evar,
concat('0',substr(CONCTNS.splitted_cnctns[24],instr(substr(CONCTNS.splitted_cnctns[24],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[24],1),'${hivevar:param2}')-1)) prv24_id,
substr(CONCTNS.splitted_cnctns[24],instr(substr(CONCTNS.splitted_cnctns[24],2),'${hivevar:param2}')+20,148) prv24_evar,
concat('0',substr(CONCTNS.splitted_cnctns[25],instr(substr(CONCTNS.splitted_cnctns[25],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[25],1),'${hivevar:param2}')-1)) prv25_id,
substr(CONCTNS.splitted_cnctns[25],instr(substr(CONCTNS.splitted_cnctns[25],2),'${hivevar:param2}')+20,148) prv25_evar,
concat('0',substr(CONCTNS.splitted_cnctns[26],instr(substr(CONCTNS.splitted_cnctns[26],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[26],1),'${hivevar:param2}')-1)) prv26_id,
substr(CONCTNS.splitted_cnctns[26],instr(substr(CONCTNS.splitted_cnctns[26],2),'${hivevar:param2}')+20,148) prv26_evar,
concat('0',substr(CONCTNS.splitted_cnctns[27],instr(substr(CONCTNS.splitted_cnctns[27],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[27],1),'${hivevar:param2}')-1)) prv27_id,
substr(CONCTNS.splitted_cnctns[27],instr(substr(CONCTNS.splitted_cnctns[27],2),'${hivevar:param2}')+20,148) prv27_evar,
concat('0',substr(CONCTNS.splitted_cnctns[28],instr(substr(CONCTNS.splitted_cnctns[28],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[28],1),'${hivevar:param2}')-1)) prv28_id,
substr(CONCTNS.splitted_cnctns[28],instr(substr(CONCTNS.splitted_cnctns[28],2),'${hivevar:param2}')+20,148) prv28_evar,
concat('0',substr(CONCTNS.splitted_cnctns[29],instr(substr(CONCTNS.splitted_cnctns[29],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[29],1),'${hivevar:param2}')-1)) prv29_id,
substr(CONCTNS.splitted_cnctns[29],instr(substr(CONCTNS.splitted_cnctns[29],2),'${hivevar:param2}')+20,148) prv29_evar,
concat('0',substr(CONCTNS.splitted_cnctns[30],instr(substr(CONCTNS.splitted_cnctns[30],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[30],1),'${hivevar:param2}')-1)) prv30_id,
substr(CONCTNS.splitted_cnctns[30],instr(substr(CONCTNS.splitted_cnctns[30],2),'${hivevar:param2}')+20,148) prv30_evar,
concat('0',substr(CONCTNS.splitted_cnctns[31],instr(substr(CONCTNS.splitted_cnctns[31],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[31],1),'${hivevar:param2}')-1)) prv31_id,
substr(CONCTNS.splitted_cnctns[31],instr(substr(CONCTNS.splitted_cnctns[31],2),'${hivevar:param2}')+20,148) prv31_evar,
concat('0',substr(CONCTNS.splitted_cnctns[32],instr(substr(CONCTNS.splitted_cnctns[32],1),'${hivevar:param3}'),instr(substr(CONCTNS.splitted_cnctns[32],1),'${hivevar:param2}')-1)) prv32_id,
substr(CONCTNS.splitted_cnctns[32],instr(substr(CONCTNS.splitted_cnctns[32],2),'${hivevar:param2}')+20,148) prv32_evar  
FROM 
(SELECT *,
split(post_product_list,'${hivevar:param3}') AS splitted_cnctns FROM ${hivevar:prod_conformed_db}.product_list_recs where
 ((lower(trim(action_type))  like '%provider_list_impression_view%' or lower(trim(action_type)) like '%provider_list_result_select%') 
  and substr(trim(post_product_list),1,5) != '${hivevar:param1}')) CONCTNS ;



 

---- provider search variable breakout, splitting each EVAR of a provider_identifier into multiple columns ----------------------------------------------------------------------



drop table ${hivevar:prod_conformed_db}.enriched_provider_search_provider_list;

create table ${hivevar:prod_conformed_db}.enriched_provider_search_provider_list  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice ,mobile_web_identifier,mobile_app_identifier,visit_start_datetime_gmt,  
prv1_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv1_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv1_id ,visit_page_number , user_visit_datetime,visit_start_datetime_gmt,  os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv1_evar , split(prv1_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv1_id is not null) CONCTNS
UNION
select visit_page_number , user_visit_datetime, proxy_identifier, action_type,  search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice, mobile_web_identifier,mobile_app_identifier,visit_start_datetime_gmt, 
prv2_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv2_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv2_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id,Provider_List_View_Type_description , typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv2_evar , split(prv2_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv2_id is not null) CONCTNS   
UNION
select visit_page_number ,user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice ,mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv3_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv3_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv3_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id,Provider_List_View_Type_description , typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv3_evar , split(prv3_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv3_id is not null) CONCTNS   
UNION
select visit_page_number ,user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier,visit_start_datetime_gmt, 
prv4_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275,prv4_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv4_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id,Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category,  prv4_evar , split(prv4_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv4_id is not null) CONCTNS   
UNION
select visit_page_number ,user_visit_datetime, proxy_identifier, action_type, search_item,   provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier,visit_start_datetime_gmt, 
prv5_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv5_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv5_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id,Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category,  prv5_evar , split(prv5_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv5_id is not null) CONCTNS   
UNION 	 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier,visit_start_datetime_gmt, 
prv6_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv6_evar post_product_line , typeahead_Search_Results_List  
FROM 
(SELECT prv6_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier,  actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv6_evar , split(prv6_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv6_id is not null) CONCTNS   	 
UNION 	 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id,Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier,visit_start_datetime_gmt, 
prv7_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275 ,prv7_evar post_product_line , typeahead_Search_Results_List
FROM 
(SELECT prv7_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier,actionTypeName action_type, provider_list_view_type, provider_list_view_type_id,Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category,  prv7_evar , split(prv7_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv7_id is not null) CONCTNS   
UNION 	 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice ,mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv8_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275,prv8_evar post_product_line , typeahead_Search_Results_List
FROM 
(SELECT prv8_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier,  actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv8_evar , split(prv8_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv8_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice ,mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv9_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv9_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv9_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id,Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category,  prv9_evar , split(prv9_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv9_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice ,mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv10_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv10_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv10_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv10_evar , split(prv10_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv10_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier,visit_start_datetime_gmt, 
prv11_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv11_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv11_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv11_evar , split(prv11_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv11_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv12_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv12_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv12_id ,visit_page_number , user_visit_datetime,visit_start_datetime_gmt,  os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv12_evar , split(prv12_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv12_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv13_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv13_evar post_product_line, typeahead_Search_Results_List 
FROM
(SELECT prv13_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv13_evar , split(prv13_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv13_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv14_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv14_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv14_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv14_evar , split(prv14_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv14_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice ,mobile_web_identifier,mobile_app_identifier,  visit_start_datetime_gmt,  
prv15_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv15_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv15_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv15_evar , split(prv15_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv15_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice ,  mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv16_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv16_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv16_id ,visit_page_number , user_visit_datetime,visit_start_datetime_gmt,  os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv16_evar , split(prv16_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv16_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice ,mobile_web_identifier,mobile_app_identifier,   visit_start_datetime_gmt, 
prv17_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv17_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv17_id ,visit_page_number , user_visit_datetime,visit_start_datetime_gmt,  os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv17_evar , split(prv17_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv17_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt,  
prv18_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv18_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv18_id ,visit_page_number , user_visit_datetime,visit_start_datetime_gmt,  os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv18_evar , split(prv18_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv18_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv19_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv19_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv19_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List,ngx_search_term search_item, ngx_search_result_category search_result_category, prv19_evar , split(prv19_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv19_id is not null) CONCTNS  
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv20_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv20_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv20_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv20_evar , split(prv20_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv20_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv21_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv21_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv21_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv21_evar , split(prv21_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv21_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv22_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv22_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv22_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv22_evar , split(prv22_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv22_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv23_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv23_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv23_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv23_evar , split(prv23_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv23_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv24_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv24_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv24_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv24_evar , split(prv24_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv24_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv25_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv25_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv25_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv25_evar , split(prv25_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv25_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv26_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv26_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv26_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv26_evar , split(prv26_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv26_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv27_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv27_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv27_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv27_evar , split(prv27_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv27_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv28_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv28_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv28_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv28_evar , split(prv28_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv28_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv29_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv29_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv29_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv29_evar , split(prv29_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv29_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv30_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv30_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv30_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv30_evar , split(prv30_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv30_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv31_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv31_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv31_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv31_evar , split(prv31_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv31_id is not null) CONCTNS   
UNION 
select visit_page_number , user_visit_datetime, proxy_identifier, action_type, search_item,  provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,os_choice , mobile_web_identifier,mobile_app_identifier, visit_start_datetime_gmt, 
prv32_id provider_identifier,
if (substr(lower(trim(CONCTNS.splitted_cnctns[0])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[0]),6)) AS Provider_View_Aexcel_1244,
if (substr(lower(trim(CONCTNS.splitted_cnctns[1])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[1]),6)) AS Provider_View_Distance_1245,
if (substr(lower(trim(CONCTNS.splitted_cnctns[2])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[2]),6)) AS In_network_badge_1246,
if (substr(lower(trim(CONCTNS.splitted_cnctns[3])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[3]),6)) AS Accepting_new_patients_badge_1247,
if (substr(lower(trim(CONCTNS.splitted_cnctns[4])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[4]),6)) AS Provider_View_Tier_1_1248,
if (substr(lower(trim(CONCTNS.splitted_cnctns[5])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[5]),6)) AS Provider_View_Tier_2_1249,
if (substr(lower(trim(CONCTNS.splitted_cnctns[6])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[6]),6)) AS ACO_icon_displayed_1250 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[7])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[7]),6)) AS Provider_View_Bridges_Excellence_1251,
if (substr(lower(trim(CONCTNS.splitted_cnctns[8])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[8]),6)) AS Provider_View_NCGA_1252 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[9])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[9]),6)) AS Provider_View_Quality_Reporting_1253 ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[10])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[10]),6)) AS Provider_1255  ,
if (substr(lower(trim(CONCTNS.splitted_cnctns[11])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[11]),6)) AS Provider_1256,
if (substr(lower(trim(CONCTNS.splitted_cnctns[12])),6,8)='::hash::',null, substr(trim(CONCTNS.splitted_cnctns[12]),6)) AS Provider_View_JV_Badge_1275, prv32_evar post_product_line, typeahead_Search_Results_List
FROM
(SELECT prv32_id ,visit_page_number , user_visit_datetime, visit_start_datetime_gmt, os_choice ,mobile_web_identifier,mobile_app_identifier,proxy_identifier, actionTypeName action_type, provider_list_view_type, provider_list_view_type_id, Provider_List_View_Type_description ,typeahead_Search_Results_List, ngx_search_term search_item, ngx_search_result_category search_result_category, prv32_evar , split(prv32_evar,'[|]')
     AS splitted_cnctns from ${hivevar:prod_conformed_db}.product_split_rec1 where prv32_id is not null) CONCTNS   ;

	 
 

---- Creating base table for Ahead searh and select criteria  with no provider details----------------------------------------------------------------------


drop table ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_search_query;


create table ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_search_query 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
SELECT 
cast(visit_page_number as INT) visit_page_number,   
cast(user_visit_datetime as timestamp) user_visit_datetime, 
cast(visit_start_datetime_gmt as timestamp) visit_start_datetime_gmt, 
proxy_identifier, 
actionTypeName action_type, 
ngx_search_term search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description,
os_choice , 
mobile_web_identifier,mobile_app_identifier,
'' provider_identifier,
'' provider_view_aexcel_1244,
'' provider_view_distance_1245,
'' in_network_badge_1246,
'' accepting_new_patients_badge_1247,
'' provider_view_tier_1_1248,
'' provider_view_tier_2_1249,
'' aco_icon_displayed_1250,
'' provider_view_bridges_excellence_1251,
'' provider_view_ncga_1252,
'' provider_view_quality_reporting_1253,
'' provider_1255,
'' provider_1256,
'' provider_view_jv_badge_1275,  
'' post_product_line,
typeahead_Search_Results_List 
from ${hivevar:prod_conformed_db}.product_list_recs where
lower(trim(action_type)) like '%aetnahealth:web:search:search_query%' and typeahead_Search_Results_List not like '%specialty%' and typeahead_Search_Results_List not like '%procedure%'; 



--only take specialty and procedures
drop table ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_1a;

create table ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_1a 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
SELECT 
cast(visit_page_number as INT) visit_page_number,   
cast(user_visit_datetime as timestamp) user_visit_datetime, 
cast(visit_start_datetime_gmt as timestamp) visit_start_datetime_gmt, 
proxy_identifier, 
actionTypeName action_type, 
ngx_search_term search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description,
os_choice , 
mobile_web_identifier,mobile_app_identifier,
'' provider_identifier,
'' provider_view_aexcel_1244,
'' provider_view_distance_1245,
'' in_network_badge_1246,
'' accepting_new_patients_badge_1247,
'' provider_view_tier_1_1248,
'' provider_view_tier_2_1249,
'' aco_icon_displayed_1250,
'' provider_view_bridges_excellence_1251,
'' provider_view_ncga_1252,
'' provider_view_quality_reporting_1253,
'' provider_1255,
'' provider_1256,
'' provider_view_jv_badge_1275,  
'' post_product_line,
typeahead_Search_Results_List 
 from ${hivevar:prod_conformed_db}.product_list_recs where 
lower(trim(action_type)) like '%aetnahealth:web:search:search_query%' and typeahead_Search_Results_List like '%specialty%'
UNION
SELECT 
cast(visit_page_number as INT) visit_page_number,   
cast(user_visit_datetime as timestamp) user_visit_datetime, 
cast(visit_start_datetime_gmt as timestamp) visit_start_datetime_gmt, 
proxy_identifier, 
actionTypeName action_type, 
ngx_search_term search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description,
os_choice , 
mobile_web_identifier,mobile_app_identifier,
'' provider_identifier,
'' provider_view_aexcel_1244,
'' provider_view_distance_1245,
'' in_network_badge_1246,
'' accepting_new_patients_badge_1247,
'' provider_view_tier_1_1248,
'' provider_view_tier_2_1249,
'' aco_icon_displayed_1250,
'' provider_view_bridges_excellence_1251,
'' provider_view_ncga_1252,
'' provider_view_quality_reporting_1253,
'' provider_1255,
'' provider_1256,
'' provider_view_jv_badge_1275,  
'' post_product_line,
typeahead_Search_Results_List 
 from ${hivevar:prod_conformed_db}.product_list_recs where 
lower(trim(action_type)) like '%aetnahealth:web:search:search_query%' and typeahead_Search_Results_List like '%procedure%';



--take other than specialty and procedure
drop table ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_result_select;

create table ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_result_select  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS  
SELECT 
cast(visit_page_number as INT) visit_page_number,   
cast(user_visit_datetime as timestamp) user_visit_datetime, 
cast(visit_start_datetime_gmt as timestamp) visit_start_datetime_gmt, 
proxy_identifier, 
actionTypeName action_type, 
ngx_search_term search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description,
os_choice , 
mobile_web_identifier,mobile_app_identifier,
'' provider_identifier,
'' provider_view_aexcel_1244,
'' provider_view_distance_1245,
'' in_network_badge_1246,
'' accepting_new_patients_badge_1247,
'' provider_view_tier_1_1248,
'' provider_view_tier_2_1249,
'' aco_icon_displayed_1250,
'' provider_view_bridges_excellence_1251,
'' provider_view_ncga_1252,
'' provider_view_quality_reporting_1253,
'' provider_1255,
'' provider_1256,
'' provider_view_jv_badge_1275,  
'' post_product_line,
typeahead_Search_Results_List 
 from ${hivevar:prod_conformed_db}.product_list_recs where  
  (lower(trim(action_type)) like '%aetnahealth:web:search:result_select%'  )  ;



--split into columns the search_results_list
drop table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1b;

create table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1b
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
select *, CONCTNS.splitted_cnctns[0],
          CONCTNS.splitted_cnctns[1],CONCTNS.splitted_cnctns[2],CONCTNS.splitted_cnctns[3],CONCTNS.splitted_cnctns[4],CONCTNS.splitted_cnctns[5],
          CONCTNS.splitted_cnctns[6],CONCTNS.splitted_cnctns[7],CONCTNS.splitted_cnctns[8],CONCTNS.splitted_cnctns[9],CONCTNS.splitted_cnctns[10],CONCTNS.splitted_cnctns[11],
          CONCTNS.splitted_cnctns[12],CONCTNS.splitted_cnctns[13],CONCTNS.splitted_cnctns[14],CONCTNS.splitted_cnctns[15],CONCTNS.splitted_cnctns[16],CONCTNS.splitted_cnctns[17],
          CONCTNS.splitted_cnctns[18],CONCTNS.splitted_cnctns[19],CONCTNS.splitted_cnctns[20],CONCTNS.splitted_cnctns[21],CONCTNS.splitted_cnctns[22],CONCTNS.splitted_cnctns[23],
          CONCTNS.splitted_cnctns[24],CONCTNS.splitted_cnctns[25],CONCTNS.splitted_cnctns[26],CONCTNS.splitted_cnctns[27],CONCTNS.splitted_cnctns[28],CONCTNS.splitted_cnctns[29],
          CONCTNS.splitted_cnctns[30]
FROM
(SELECT *,
split(typeahead_search_results_list,'--') AS splitted_cnctns FROM ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_1a ) CONCTNS;

 

--spilt into rows
drop table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1c;

create table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1c 
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT 
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
os_choice , mobile_web_identifier,mobile_app_identifier,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,   
typeahead_Search_Results_List , 
splitted_cnctns[0] Search_result_id, 
substr(splitted_cnctns[0],instr(substr(splitted_cnctns[0],1),':')+1) Search_result_id_1
FROM ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1b where splitted_cnctns[0] is not null 
UNION 
SELECT 
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
os_choice , mobile_web_identifier,mobile_app_identifier,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,    
typeahead_Search_Results_List , 
splitted_cnctns[2] Search_result_id, 
substr(splitted_cnctns[2],instr(substr(splitted_cnctns[2],1),':')+1) Search_result_id_1 
FROM ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1b where splitted_cnctns[2] is not null 
UNION 
SELECT  
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
os_choice , mobile_web_identifier,mobile_app_identifier,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,    
typeahead_Search_Results_List , 
splitted_cnctns[4] Search_result_id, 
substr(splitted_cnctns[4],instr(substr(splitted_cnctns[4],1),':')+1) Search_result_id_1
FROM ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1b  where splitted_cnctns[4] is not null 
UNION 
SELECT  
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
os_choice , mobile_web_identifier,mobile_app_identifier,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,    
typeahead_Search_Results_List , 
splitted_cnctns[6] Search_result_id, 
substr(splitted_cnctns[6],instr(substr(splitted_cnctns[6],1),':')+1) Search_result_id_1
FROM ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1b  where splitted_cnctns[6] is not null 
UNION 
SELECT  
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
os_choice , mobile_web_identifier,mobile_app_identifier,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,    
typeahead_Search_Results_List , 
splitted_cnctns[8] Search_result_id, 
substr(splitted_cnctns[8],instr(substr(splitted_cnctns[8],1),':')+1) Search_result_id_1
FROM ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1b  where splitted_cnctns[8] is not null 
UNION 
SELECT  
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
os_choice , mobile_web_identifier,mobile_app_identifier,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,    
typeahead_Search_Results_List , 
splitted_cnctns[10] Search_result_id, 
substr(splitted_cnctns[10],instr(substr(splitted_cnctns[10],1),':')+1) Search_result_id_1
FROM ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1b  where splitted_cnctns[10] is not null 
order by 
user_visit_datetime,   
visit_page_number,   
proxy_identifier
;


--only take the spilt rows for specialty and procedure codes
drop table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1d;

create table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1d  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
select 
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
os_choice ,  mobile_web_identifier,mobile_app_identifier,
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,    
Search_result_id, 
Search_result_id_1,
a.name search_item_name,
typeahead_Search_Results_List 
FROM  ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1c  
LEFT OUTER JOIN ${hivevar:prod_conformed_db}.clinical_specialties a 
ON a.id = search_result_id_1 and search_result_id like '%specialty%'
where search_result_id like '%specialty%' 
UNION 
select 
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
os_choice ,  mobile_web_identifier,mobile_app_identifier,
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,
Search_result_id, 
Search_result_id_1,
b.name search_item_name,  
typeahead_Search_Results_List
FROM  ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1c  
LEFT OUTER JOIN ${hivevar:prod_conformed_db}.clinical_procedures  b
ON b.id = search_result_id_1 and search_result_id like '%procedure%'
where search_result_id like '%procedure%';


--Merge all records for Ahad search Query
drop table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1e;

create table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1e  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")
AS
SELECT 
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
os_choice ,  mobile_web_identifier,mobile_app_identifier,
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,
Search_result_id, 
Search_result_id_1,
search_item_name,  
typeahead_Search_Results_List 
FROM ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1d 
UNION
SELECT 
visit_page_number,   
user_visit_datetime, 
visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description,
os_choice , mobile_web_identifier,mobile_app_identifier,
'' provider_identifier,
'' provider_view_aexcel_1244,
'' provider_view_distance_1245,
'' in_network_badge_1246,
'' accepting_new_patients_badge_1247,
'' provider_view_tier_1_1248,
'' provider_view_tier_2_1249,
'' aco_icon_displayed_1250,
'' provider_view_bridges_excellence_1251,
'' provider_view_ncga_1252,
'' provider_view_quality_reporting_1253,
'' provider_1255,
'' provider_1256,
'' provider_view_jv_badge_1275,  
'' post_product_line,
'' Search_result_id, 
'' Search_result_id_1,
'' search_item_name,  
typeahead_Search_Results_List 
FROM ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_search_query ;




---- Merging all events for Provider search criteria  ----------------------------------------------------------------------
--Merge all records for Ahead search Query and Ahead serach result
--Merge all records forprovider list




drop table ${hivevar:prod_conformed_db}.enriched_provider_search_nf;

create table ${hivevar:prod_conformed_db}.enriched_provider_search_nf  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS
SELECT 
cast(visit_page_number as INT) visit_page_number,   
cast(user_visit_datetime as timestamp) user_visit_datetime, 
cast(visit_start_datetime_gmt as timestamp) visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description,
os_choice ,  mobile_web_identifier,mobile_app_identifier,
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,
Search_result_id,  
search_item_name search_result_name,  
typeahead_Search_Results_List 
FROM ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1e 
UNION
SELECT 
cast(visit_page_number as INT) visit_page_number,   
cast(user_visit_datetime as timestamp) user_visit_datetime, 
cast(visit_start_datetime_gmt as timestamp) visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description, 
os_choice , mobile_web_identifier,mobile_app_identifier,
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line ,
'' Search_result_id,  
'' search_result_name,  
'' typeahead_Search_Results_List 
FROM ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_result_select   
UNION
select 
cast(visit_page_number as INT) visit_page_number,   
cast(user_visit_datetime as timestamp) user_visit_datetime, 
cast(visit_start_datetime_gmt as timestamp) visit_start_datetime_gmt, 
proxy_identifier, 
action_type, 
search_item search_term,
provider_list_view_type,
provider_list_view_type_id,
provider_list_view_type_description,
os_choice , mobile_web_identifier,mobile_app_identifier,
provider_identifier,
provider_view_aexcel_1244,
provider_view_distance_1245,
in_network_badge_1246,
accepting_new_patients_badge_1247,
provider_view_tier_1_1248,
provider_view_tier_2_1249,
aco_icon_displayed_1250,
provider_view_bridges_excellence_1251,
provider_view_ncga_1252,
provider_view_quality_reporting_1253,
provider_1255,
provider_1256,
provider_view_jv_badge_1275,  
post_product_line , 
'' Search_result_id,  
'' search_result_name,  
'' typeahead_Search_Results_List  
from ${hivevar:prod_conformed_db}.enriched_provider_search_provider_list   ;



drop table ${hivevar:prod_conformed_db}.enriched_provider_search;

create table ${hivevar:prod_conformed_db}.enriched_provider_search 
STORED AS ORC tblproperties("orc.compress"="SNAPPY")   
AS
SELECT * 
FROM ${hivevar:prod_conformed_db}.enriched_provider_search_nf 
order by 
user_visit_datetime,visit_page_number,proxy_identifier;








---- Creating Provider extract file ----------------------------------------------------------------------



---- Removing temporary tables ---------------------------------------------------------------------------



drop table ${hivevar:prod_conformed_db}.enriched_provider_search_provider_list purge;
drop table ${hivevar:prod_conformed_db}.extract_provider_search_1a purge;
drop table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1a purge;
drop table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1b purge;
drop table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1c purge;
drop table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1d purge;
drop table ${hivevar:prod_conformed_db}.product_list_recs_split_prvs_1e purge;
drop table ${hivevar:prod_conformed_db}.product_split_rec1 purge;

drop table ${hivevar:prod_conformed_db}.product_split_rec1 purge;
drop table ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_search_query purge;
drop table ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_1a purge;
drop table ${hivevar:prod_conformed_db}.product_list_recs_no_prvs_result_select purge;
 

drop table ${hivevar:prod_conformed_db}.product_list_recs purge;
drop table ${hivevar:prod_conformed_db}.product_get_recs purge;
