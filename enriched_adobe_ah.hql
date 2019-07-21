------------------------------------------------------------------------------------------------------------
--                                        Adobe Ngx                                                     ----     
------------------------------------------------------------------------------------------------------------
-- This scripts builds a enriched view for adobe ngx Aetna Health data                                 ----
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
--set hivevar:prod_conformed_db=BI_NGX_PROD_ENC;
--set prod_conformed_db; 
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

    


----Decryption of post_evar124 membership id----------------------------------------------------------------------
--Added UDF function for decryption  
--!run /var/webeng/hadoop/voltage_udf/createudf.hql
 
--DROP TABLE IF EXISTS ${prod_conformed_db}.tmp_abobe_ngx_membership_124 purge;
--CREATE  TABLE ${prod_conformed_db}.tmp_abobe_ngx_membership_124 
--STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
--AS
--SELECT distinct post_evar124 from ${adobe_db}.adobe_ngx
--where trim(post_evar124) != '';  
 

--DROP TABLE IF EXISTS ${prod_stg_db}.member_decrypted_all_124;
--Create TEMPORARY table ${prod_stg_db}.member_decrypted_all_124
--STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
--AS
--SELECT  
--post_evar124,  
--Upper(accessdata(trim(post_evar124),'NAME2'))  AS  decrypted_membership
--FROM ${prod_stg_db}.tmp_abobe_ngx_membership_124 ;



------Create Generic Enriched Adobe NGX layer---------------------------------------------------------------------
 
--CREATE  TABLE IF NOT EXISTS ${hivevar:prod_conformed_db}.stg_enriched_adobe_ngx  
--STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
--AS 
--SELECT * from ${hivevar:prod_conformed_db}.conformed_adobe_ngx where 1=0;



--INSERT INTO  ${hivevar:prod_conformed_db}.stg_enriched_adobe_ngx  
--SELECT *      
--FROM  ${hivevar:prod_conformed_db}.stg_conformed_adobe_ngx_1;



--------------------------------------------------------------------------------------------------------------
--Fetching individual_id from persona_proxy using proxy_id
--Get the latest records of preferred proxy ids and individual_analytics_id_source from the persona proxy table
---------------------------------------------------------------------------------------------------------------

--DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.persona_proxy_ah purge;
--CREATE  TABLE ${hivevar:prod_conformed_db}.persona_proxy_ah 
--stored AS orc tblproperties("orc.compress"="SNAPPY") 
--AS
--SELECT individual_id, max(proxy_id) as proxy_id, concat_ws(',', collect_set(individual_analytics_id_source)) as individual_analytics_id_source
--FROM   
--    ( 
--         SELECT 
--               individual_id,   
--               proxy_id,
--              individual_analytics_id_source,
--               rank() OVER(partition BY proxy_id ORDER BY business_effective_date desc, business_expiration_date desc) AS visittime 
--               from ${hivevar:source_conformed_db}.persona_proxy           where preferred_indicator = 'Y' 
--        )rk 
--WHERE rk.visittime=1 and trim(proxy_id)<>'' and proxy_id is not null group by individual_id ;



----GET appropriate individual_id, proxy_id  and source added into Enriched AH dataset from Conformed AH dataset -----------------------------------------------


 
DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.enriched_session_2 PURGE;
CREATE  TABLE ${hivevar:prod_conformed_db}.enriched_session_2
stored AS orc tblproperties("orc.compress"="SNAPPY") 
AS 
SELECT 
a.*,  
case when trim(substr(a.source_proxy_identifier,1,3)) = '15~' then trim(substr(a.source_proxy_identifier,4)) else trim(a.source_proxy_identifier) end as ah_proxy_id   
FROM ${hivevar:prod_conformed_db}.stg_conformed_adobe_ah a  
WHERE  load_year=year(CURRENT_DATE) 
AND    load_month=month(CURRENT_DATE) 
AND    load_day=day(CURRENT_DATE) ;




DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.persona_proxy_for_enriched_session_3 PURGE;
CREATE  TABLE ${hivevar:prod_conformed_db}.persona_proxy_for_enriched_session_3
stored AS orc tblproperties("orc.compress"="SNAPPY") 
AS 
SELECT 
a.*, 
ah_proxy_id preferred_proxy_id  ,
'' individual_analytics_id_source 
FROM  ${hivevar:prod_conformed_db}.enriched_session_2 a ; 
--LEFT  OUTER JOIN ${hivevar:prod_conformed_db}.persona_proxy_ah  b 
--ON   trim(a.ah_proxy_id) = Trim(b.proxy_id); 

--Example:
--MEA (Aetna Commerical membership)
--HMO(Aetna Medicare Membership (which can be identified by the first 2 bytes of individual_analytics_identifier='ME')), 
--HRP (Consumer Business, like individual medicare membership)
--HMO (Aetna Medicare Membership -- (which can be identified by the first 2 bytes of individual_analytics_identifier='ME')), 
--Coventry Medicare membership -- 1010598465
 

DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.persona_proxy_for_enriched_session_3a PURGE;
CREATE  TABLE ${hivevar:prod_conformed_db}.persona_proxy_for_enriched_session_3a
stored AS orc tblproperties("orc.compress"="SNAPPY") 
AS  
SELECT  
a.*,  
CASE WHEN individual_analytics_id_source like '%MEA%' THEN 'MEA'
     WHEN individual_analytics_id_source like '%HMO%' THEN 'HMO' 
     WHEN individual_analytics_id_source like '%HRP%' THEN 'HRP' 
     WHEN individual_analytics_id_source like '%CVH%' THEN 'CVH'  
     WHEN individual_analytics_id_source like '%PFX%' THEN 'PFX'
    ELSE '' 
END AS individual_analytics_id_source_comb , 
CASE when user_agent like '%Mac OS X%' then 'iOS' 
     when user_agent like '%Android%'  then 'Android' 
     when user_agent like '%Windows%'  then 'Windows' 
     else 'Other' end as OS_choice  , 
   From_unixtime(Cast(a.visit_start_time_gmt_unix AS    INT))                                      AS visit_start_datetime_gmt,
   From_unixtime(Cast(a.first_hit_time_gmt_unix AS     INT))                                      AS first_hit_datetime_gmt ,
   From_unixtime(Cast(a.hit_time_gmt_unix AS           INT))                                      AS hit_datetime_gmt ,
   From_unixtime(Cast(a.last_hit_time_gmt_unix AS      INT))                                      AS last_hit_datetime_gmt ,
   From_unixtime(Cast(a.post_cust_hit_time_gmt_unix AS INT))                                      AS post_cust_hit_datetime_gmt ,
   From_unixtime(Cast(a.cust_hit_time_gmt_unix AS      INT))                                      AS cust_hit_datetime_gmt,
   user_visit_datetime as visit_start_datetime, 
   lead(user_visit_datetime, 1, 'NA') over (partition by marketing_cloud_visit_identifier, post_visid_high, post_visid_low, visit_number
   ORDER BY marketing_cloud_visit_identifier, post_visid_high, post_visid_low, cast(visit_number as int), cast(visit_page_number as int)) as visit_end_datetime  
FROM    ${hivevar:prod_conformed_db}.persona_proxy_for_enriched_session_3 a  ;







-- Creating staging enriched table. This steps arranges the columns as per data model----------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.tmp_enriched_ngx_interaction_10 PURGE;
CREATE   TABLE ${hivevar:prod_conformed_db}.tmp_enriched_ngx_interaction_10 
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS
select 
--a.individual_id as individual_identifier ,   
a.individual_analytics_identifier as individual_analytics_identifier              ,
a.individual_analytics_id_source_comb as individual_analytics_identifier_source   ,
a.interaction_type                                ,
a.interaction_source                              ,
a.preferred_proxy_id as preferred_proxy_identifier, 
a.source_individual_identifier                    ,
a.source_alternate_identifier                     ,
a.source_subscriber_cumb_identifier               ,
a.source_proxy_identifier                         ,
a.source_member_identifier                        ,
a.first_name                                      ,
a.last_name                                       ,
a.email_address                                   ,
a.zip_code                                        ,
a.id_card_type                                    ,
a.session_identifier                              ,
a.download_file_name                              ,
a.previous_page_name                              ,
a.page_name                                       ,
a.form_name                                       ,
a.page_url                                        ,
a.mobile_identifier                               ,
a.dependent_indicator                             ,
a.marketing_cloud_visit_identifier                ,
a.visit_number                                    ,
a.hit_identifier_high                             ,
a.hit_identifier_low                              ,
a.visit_start_page_name                           ,
a.visit_page_number                               ,
a.visitor_identifier                              ,
a.hit_source                                      ,
a.visit_start_time_gmt_unix                       ,
a.visid_high                                      ,
a.visid_low                                       ,
a.post_visid_high                                 ,
a.post_visid_low                                  ,
a.exclude_hit                                     ,
a.post_prop2                                      ,
a.post_prop1                                      ,
a.user_visit_datetime                             ,
a.user_identifier                                 ,
a.user_name                                       ,
a.user_birth_date                                 ,
a.user_gender_source                              ,
a.user_zip_code                                   ,
a.user_email_address                              ,
a.user_authentication_status                      ,
a.user_login_timestamp                            ,
a.user_agent                                      ,
a.login_sso_indicator                             ,
a.login_my_assistant                              ,
a.profile_ethnicity_code_source                   ,
--a.profile_ethnicity_code_description              ,
--a.profile_primary_spoken_language_code_source     ,
--a.profile_primary_spoken_language_description     ,
--a.profile_primary_written_language_code_source    ,
--a.profile_primary_written_language_description    ,
a.post_event_list                                 ,
a.event_list                                      ,
a.page_event                                      ,
a.post_page_event                                 ,
a.post_product_list                               ,
a.post_page_name                                  ,
a.post_page_url                                   ,
a.interaction_generated_error_text                ,
a.generic_link_name                               ,
a.pct_of_page_viewed                              ,
a.query_string                                    ,
a.input_filter_name                               ,
a.input_filter_value                              ,
a.claim_name                                      ,
a.automatic_login_indicator                       ,
a.source_language_code                            ,
--a.language_description                            ,
a.country_identifier_source                       ,
--a.country_description                             ,
a.state_abbreviation                              ,
a.channel_description                             ,
a.duplicate_purchase                              ,
a.email_identifier                                ,
a.email_campaign_identifier                       ,
a.registration_first_name                         ,
a.registration_last_name                          ,
a.search_results_count                            ,
a.registration_step_name                          ,
a.registration_id_type                            ,
a.registration_flow_type                          ,
a.input_doc_find_search_term                      ,
a.input_doc_find_distance                         ,
a.ol_find_what_looking_for_indicator              ,
a.input_doc_find_zip_code                         ,
a.input_doc_find_type_of_search                   ,
a.qualtrics_respondent_identifier                 ,
a.ol_value_of_text_box                            ,
a.ol_value_of_ask                                 ,
a.ol_able_to_register_login_indicator             ,
a.ol_value_of_information                         ,
a.ol_overall_satisfaction                         ,
a.ol_need_call_customer_service_indicator         ,
a.ol_ease_of_use                                  ,
a.ol_comments_left_indicator                      ,
a.ol_appearance                                   ,
a.ngx_search_term,
a.ngx_search_result_identifier ,
a.ngx_search_result_category,
a.ngx_membership_identifier,
a.ngx_claim_detail_identifier,
a.mpe_distance                                    ,
a.mpe_search_category                             ,
a.mpe_search_result_count                         ,
a.mpe_search_term                                 ,
a.mpe_zip_code                                    ,
a.mpe_member_name                                 ,
a.mpe_facility_search_term                        ,
a.member_age_during_interaction                   ,
a.organization_identifier                         ,
a.organization_arrangement_identifier             ,
a.plan_sponsor_identifier                         ,
a.plan_sponsor_name                               ,
--a.accept_language_description                     ,
a.browser_identifier                              ,
--a.browser_identifier_description                  ,
a.browser_height                                  ,
a.browser_width                                   ,
a.mobile_carrier                                  ,
a.clickmap_click_action                           ,
a.clickmap_click_action_type                      ,
a.clickmap_click_context                          ,
a.clickmap_click_context_type                     ,
a.clickmap_click_source_identifier                ,
a.clickmap_click_tag                              ,
a.code_version                                    ,
a.color_identifier                                ,
--a.color_identifier_description                    ,
a.connection_type_identifier                      ,
--a.connection_type_identifier_description          ,
a.cookies_indicator                               ,
a.currency_factor                                 ,
a.currency_rate                                   ,
a.cust_hit_time_gmt_unix                          ,
a.daily_visitor                     ,
a.user_domain                                     ,
a.first_hit_page_url                              ,
a.first_hit_page_name                             ,
a.first_hit_referrer_type_code                    ,
--a.first_hit_referrer_type_description             ,
a.first_hit_referrer                              ,
a.first_hit_time_gmt_unix                         ,
a.geo_city                                        ,
a.geo_country                                     ,
a.geo_designated_market_area                      ,
a.geo_region                                      ,
a.geo_zip_code                                    ,
a.hit_time_gmt_unix                               ,
a.home_page_indicator                             ,
a.new_hourly_visitor_indicator                    ,
a.ip_address                                      ,
a.java_jscript_browser_version                    ,
a.java_enabled_indicator                          ,
a.java_script_version_code                        ,
--a.java_script_version_description                 ,
a.last_hit_time_gmt_unix                          ,
a.last_purchase_number                            ,
a.last_purchase_time_gmt                          ,
a.monthly_visitor_indicator                       ,
a.new_visit_indicator                             ,
a.operating_system_identifier                     ,
--a.operating_system_identifier_description         ,
a.page_event_var1                                 ,
a.page_event_var2                                 ,
a.paid_search_indicator                           ,
a.persistent_cookie_indicator                     ,
a.post_browser_height                             ,
a.post_browser_width                              ,
a.post_channel                                    ,
a.post_cookies_indicator                          ,
a.post_currency                                   ,
a.post_cust_hit_time_gmt_unix                     ,
a.post_java_enabled_indicator                     ,
a.post_page_event_var1                            ,
a.post_page_event_var2                            ,
a.post_page_name_no_url                           ,
a.post_persistent_cookie_indicator                ,
a.post_referrer                                   ,
a.post_search_engine                              ,
a.post_t_time_info                                ,
a.post_visid_type_code                            ,
--a.post_visid_type_description                     ,
a.post_zip_code                                   ,
a.quarterly_visitor_indicator                     ,
a.referral_type_code                              ,
--a.referral_type_description                       ,
a.referrer                                        ,
a.resolution_identifier                           ,
--a.resolution_identifier_description               ,
a.screen_resolution                               ,
a.sampled_hit_indicator                           ,
a.search_engine_identifier                        ,
--a.search_engine_identifier_description            ,
a.search_page_number                              ,
a.secondary_hit                                   ,
a.source_identifier                               ,
a.stats_server                                    ,
a.visitor_time_information                        ,
a.truncated_hit_indicator                         ,
a.user_hash                                       ,
a.value_closer_identifier                         ,
a.value_finder_identifier                         ,
a.value_instance_event                            ,
a.value_new_engagement                            ,
a.visid_new_indicator                             ,
a.visid_type_code                                 ,
--a.visid_type_description                          ,
a.visit_referral_type_code                        ,
--a.visit_referral_type_description                 ,
a.visit_referrer                                  ,
a.visit_search_engine                             ,
a.weekly_visitor_indicator                        ,
a.yearly_visitor_indicator                        ,
a.row_created_timestamp                           ,
a.row_created_by                                  ,
a.data_owner_entity                               , 
a.visit_start_datetime_gmt                        ,
a.first_hit_datetime_gmt                          ,
a.hit_datetime_gmt                                ,
a.last_hit_datetime_gmt                           ,
a.post_cust_hit_datetime_gmt                      ,
a.cust_hit_datetime_gmt                           ,
a.visit_start_datetime                            ,
a.visit_end_datetime                              ,
(unix_timestamp(visit_end_datetime) - unix_timestamp(visit_start_datetime)) as  total_visit_time_seconds,
a.Provider_List_View_Type                         , 
a.Provider_List_View_Type_ID                      ,  
a.product_list                                    , 
a.typeahead_Search_Results_List                   , 
a.mobile_web,
a.OS_choice, 
a.post_mobile_appid  , 
year(current_date) as load_year,
month(current_date) as load_month,
day(current_date) as load_day 
from  ${hivevar:prod_conformed_db}.persona_proxy_for_enriched_session_3a  a; 





-- Creating staging target table which is partitioned -----------------------------------------------------------------------------



DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.stg_enriched_adobe_ah PURGE;
CREATE TABLE  ${hivevar:prod_conformed_db}.stg_enriched_adobe_ah ( 
--individual_identifier STRING ,    
individual_analytics_identifier STRING,
individual_analytics_identifier_source STRING,
interaction_type STRING,
interaction_source STRING,
preferred_proxy_identifier STRING,
source_individual_identifier STRING,
source_alternate_identifier STRING,
source_subscriber_cumb_identifier STRING,
source_proxy_identifier STRING,
source_member_identifier STRING,
first_name STRING,
last_name STRING,
email_address STRING,
zip_code STRING,
id_card_type STRING,
session_identifier STRING,
download_file_name STRING,
previous_page_name STRING,
page_name STRING,
form_name STRING,
page_url STRING,
mobile_identifier STRING,
dependent_indicator STRING,
marketing_cloud_visit_identifier STRING,
visit_number STRING,
hit_identifier_high STRING,
hit_identifier_low STRING,
visit_start_page_name STRING,
visit_page_number STRING,
visitor_identifier STRING,
hit_source STRING,
visit_start_time_gmt_unix STRING,
visid_high STRING,
visid_low STRING,
post_visid_high STRING,
post_visid_low STRING,
exclude_hit STRING,
post_prop2 STRING,
post_prop1 STRING,
user_visit_datetime TIMESTAMP,
user_identifier STRING,
user_name STRING,
user_birth_date STRING,
user_gender_source STRING,
user_zip_code STRING,
user_email_address STRING,
user_authentication_status STRING,
user_login_timestamp STRING,
user_agent STRING,
login_sso_indicator STRING,
login_my_assistant STRING,
profile_ethnicity_code_source STRING,
--profile_ethnicity_code_description STRING,
--profile_primary_spoken_language_code_source STRING,
--profile_primary_spoken_language_description STRING,
--profile_primary_written_language_code_source STRING,
--profile_primary_written_language_description STRING,
post_event_list STRING,
event_list STRING,
page_event STRING,
post_page_event STRING,
post_product_list STRING,
post_page_name STRING,
post_page_url STRING,
interaction_generated_error_text STRING,
generic_link_name STRING,
pct_of_page_viewed STRING,
query_string STRING,
input_filter_name STRING,
input_filter_value STRING,
claim_name STRING,
automatic_login_indicator STRING,
source_language_code STRING,
--language_description STRING,
country_identifier_source STRING,
--country_description STRING,
state_abbreviation STRING,
channel_description STRING,
duplicate_purchase STRING,
email_identifier STRING,
email_campaign_identifier STRING,
registration_first_name STRING,
registration_last_name STRING,
search_results_count STRING,
registration_step_name STRING,
registration_id_type STRING,
registration_flow_type STRING,
input_doc_find_search_term STRING,
input_doc_find_distance STRING,
ol_find_what_looking_for_indicator STRING,
input_doc_find_zip_code STRING,
input_doc_find_type_of_search STRING,
qualtrics_respondent_identifier STRING,
ol_value_of_text_box STRING,
ol_value_of_ask STRING,
ol_able_to_register_login_indicator STRING,
ol_value_of_information STRING,
ol_overall_satisfaction STRING,
ol_need_call_customer_service_indicator STRING,
ol_ease_of_use STRING,
ol_comments_left_indicator STRING,
ol_appearance STRING,
ngx_search_term STRING,
ngx_search_result_identifier STRING,
ngx_search_result_category STRING,
ngx_membership_identifier STRING,
ngx_claim_detail_identifier STRING,
mpe_distance STRING,
mpe_search_category STRING,
mpe_search_result_count STRING,
mpe_search_term STRING,
mpe_zip_code STRING,
mpe_member_name STRING,
mpe_facility_search_term STRING,
member_age_during_interaction STRING,
organization_identifier STRING,
organization_arrangement_identifier STRING,
plan_sponsor_identifier STRING,
plan_sponsor_name STRING,
--accept_language_description STRING,
browser_identifier STRING,
--browser_identifier_description STRING,
browser_height STRING,
browser_width STRING,
mobile_carrier STRING,
clickmap_click_action STRING,
clickmap_click_action_type STRING,
clickmap_click_context STRING,
clickmap_click_context_type STRING,
clickmap_click_source_identifier STRING,
clickmap_click_tag STRING,
code_version STRING,
color_identifier STRING,
--color_identifier_description STRING,
connection_type_identifier STRING,
--connection_type_identifier_description STRING,
cookies_indicator STRING,
currency_factor STRING,
currency_rate STRING,
cust_hit_time_gmt_unix STRING,
daily_visitor STRING,
user_domain STRING,
first_hit_page_url STRING,
first_hit_page_name STRING,
first_hit_referrer_type_code STRING,
--first_hit_referrer_type_description STRING,
first_hit_referrer STRING,
first_hit_time_gmt_unix STRING,
geo_city STRING,
geo_country STRING,
geo_designated_market_area STRING,
geo_region STRING,
geo_zip_code STRING,
hit_time_gmt_unix STRING,
home_page_indicator STRING,
new_hourly_visitor_indicator STRING,
ip_address STRING,
java_jscript_browser_version STRING,
java_enabled_indicator STRING,
java_script_version_code STRING,
--java_script_version_description STRING,
last_hit_time_gmt_unix STRING,
last_purchase_number STRING,
last_purchase_time_gmt STRING,
monthly_visitor_indicator STRING,
new_visit_indicator STRING,
operating_system_identifier STRING,
--operating_system_identifier_description STRING,
page_event_var1 STRING,
page_event_var2 STRING,
paid_search_indicator STRING,
persistent_cookie_indicator STRING,
post_browser_height STRING,
post_browser_width STRING,
post_channel STRING,
post_cookies_indicator STRING,
post_currency STRING,
post_cust_hit_time_gmt_unix STRING,
post_java_enabled_indicator STRING,
post_page_event_var1 STRING,
post_page_event_var2 STRING,
post_page_name_no_url STRING,
post_persistent_cookie_indicator STRING,
post_referrer STRING,
post_search_engine STRING,
post_t_time_info STRING,
post_visid_type_code STRING,
--post_visid_type_description STRING,
post_zip_code STRING,
quarterly_visitor_indicator STRING,
referral_type_code STRING,
--referral_type_description STRING,
referrer STRING,
resolution_identifier STRING,
--resolution_identifier_description STRING,
screen_resolution STRING,
sampled_hit_indicator STRING,
search_engine_identifier STRING,
--search_engine_identifier_description STRING,
search_page_number STRING,
secondary_hit STRING,
source_identifier STRING,
stats_server STRING,
visitor_time_information STRING,
truncated_hit_indicator STRING,
user_hash STRING,
value_closer_identifier STRING,
value_finder_identifier STRING,
value_instance_event STRING,
value_new_engagement STRING,
visid_new_indicator STRING,
visid_type_code STRING,
--visid_type_description STRING,
visit_referral_type_code STRING,
--visit_referral_type_description STRING,
visit_referrer STRING,
visit_search_engine STRING,
weekly_visitor_indicator STRING,
yearly_visitor_indicator STRING,
row_created_timestamp STRING,
row_created_by STRING, 
data_owner_entity STRING,  
visit_start_datetime_gmt TIMESTAMP,
first_hit_datetime_gmt TIMESTAMP,
hit_datetime_gmt TIMESTAMP,
last_hit_datetime_gmt TIMESTAMP,
post_cust_hit_datetime_gmt TIMESTAMP,
cust_hit_datetime_gmt TIMESTAMP,
visit_start_datetime TIMESTAMP,
visit_end_datetime TIMESTAMP,
total_visit_time_seconds STRING,
Provider_List_View_Type  STRING, 
Provider_List_View_Type_ID STRING,  
product_list               STRING,
typeahead_Search_Results_List  STRING,
mobile_web STRING,
OS_choice STRING ,
post_mobile_appid  STRING 
)
PARTITIONED BY (
load_year string,
load_month string,
load_day string)
STORED AS ORC tblproperties("orc.compress"="SNAPPY")  ; 

 

----Loading data into staging target table-----------------------------------------------------------------------------



INSERT overwrite TABLE ${hivevar:prod_conformed_db}.stg_enriched_adobe_ah partition 
       ( 
              load_year, 
              load_month , 
              load_day 
       ) 
SELECT * FROM ${hivevar:prod_conformed_db}.tmp_enriched_ngx_interaction_10;





----Creating target table which is partitioned-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS ${hivevar:prod_conformed_db}.enriched_adobe_ah ( 
--individual_identifier  STRING ,    
individual_analytics_identifier STRING,
individual_analytics_identifier_source STRING,
interaction_type STRING,
interaction_source STRING,
preferred_proxy_identifier STRING,
source_individual_identifier STRING,
source_alternate_identifier STRING,
source_subscriber_cumb_identifier STRING,
source_proxy_identifier STRING,
source_member_identifier STRING,
first_name STRING,
last_name STRING,
email_address STRING,
zip_code STRING,
id_card_type STRING,
session_identifier STRING,
download_file_name STRING,
previous_page_name STRING,
page_name STRING,
form_name STRING,
page_url STRING,
mobile_identifier STRING,
dependent_indicator STRING,
marketing_cloud_visit_identifier STRING,
visit_number STRING,
hit_identifier_high STRING,
hit_identifier_low STRING,
visit_start_page_name STRING,
visit_page_number STRING,
visitor_identifier STRING,
hit_source STRING,
visit_start_time_gmt_unix STRING,
visid_high STRING,
visid_low STRING,
post_visid_high STRING,
post_visid_low STRING,
exclude_hit STRING,
post_prop2 STRING,
post_prop1 STRING,
user_visit_datetime TIMESTAMP,
user_identifier STRING,
user_name STRING,
user_birth_date STRING,
user_gender_source STRING,
user_zip_code STRING,
user_email_address STRING,
user_authentication_status STRING,
user_login_timestamp STRING,
user_agent STRING,
login_sso_indicator STRING,
login_my_assistant STRING,
profile_ethnicity_code_source STRING,
--profile_ethnicity_code_description STRING,
--profile_primary_spoken_language_code_source STRING,
--profile_primary_spoken_language_description STRING,
--profile_primary_written_language_code_source STRING,
--profile_primary_written_language_description STRING,
post_event_list STRING,
event_list STRING,
page_event STRING,
post_page_event STRING,
post_product_list STRING,
post_page_name STRING,
post_page_url STRING,
interaction_generated_error_text STRING,
generic_link_name STRING,
pct_of_page_viewed STRING,
query_string STRING,
input_filter_name STRING,
input_filter_value STRING,
claim_name STRING,
automatic_login_indicator STRING,
source_language_code STRING,
--language_description STRING,
country_identifier_source STRING,
--country_description STRING,
state_abbreviation STRING,
channel_description STRING,
duplicate_purchase STRING,
email_identifier STRING,
email_campaign_identifier STRING,
registration_first_name STRING,
registration_last_name STRING,
search_results_count STRING,
registration_step_name STRING,
registration_id_type STRING,
registration_flow_type STRING,
input_doc_find_search_term STRING,
input_doc_find_distance STRING,
ol_find_what_looking_for_indicator STRING,
input_doc_find_zip_code STRING,
input_doc_find_type_of_search STRING,
qualtrics_respondent_identifier STRING,
ol_value_of_text_box STRING,
ol_value_of_ask STRING,
ol_able_to_register_login_indicator STRING,
ol_value_of_information STRING,
ol_overall_satisfaction STRING,
ol_need_call_customer_service_indicator STRING,
ol_ease_of_use STRING,
ol_comments_left_indicator STRING,
ol_appearance STRING,
ngx_search_term STRING,
ngx_search_result_identifier STRING,
ngx_search_result_category STRING,
ngx_membership_identifier STRING,
ngx_claim_detail_identifier STRING,
mpe_distance STRING,
mpe_search_category STRING,
mpe_search_result_count STRING,
mpe_search_term STRING,
mpe_zip_code STRING,
mpe_member_name STRING,
mpe_facility_search_term STRING,
member_age_during_interaction STRING,
organization_identifier STRING,
organization_arrangement_identifier STRING,
plan_sponsor_identifier STRING,
plan_sponsor_name STRING,
--accept_language_description STRING,
browser_identifier STRING,
--browser_identifier_description STRING,
browser_height STRING,
browser_width STRING,
mobile_carrier STRING,
clickmap_click_action STRING,
clickmap_click_action_type STRING,
clickmap_click_context STRING,
clickmap_click_context_type STRING,
clickmap_click_source_identifier STRING,
clickmap_click_tag STRING,
code_version STRING,
color_identifier STRING,
--color_identifier_description STRING,
connection_type_identifier STRING,
--connection_type_identifier_description STRING,
cookies_indicator STRING,
currency_factor STRING,
currency_rate STRING,
cust_hit_time_gmt_unix STRING,
daily_visitor STRING,
user_domain STRING,
first_hit_page_url STRING,
first_hit_page_name STRING,
first_hit_referrer_type_code STRING,
--first_hit_referrer_type_description STRING,
first_hit_referrer STRING,
first_hit_time_gmt_unix STRING,
geo_city STRING,
geo_country STRING,
geo_designated_market_area STRING,
geo_region STRING,
geo_zip_code STRING,
hit_time_gmt_unix STRING,
home_page_indicator STRING,
new_hourly_visitor_indicator STRING,
ip_address STRING,
java_jscript_browser_version STRING,
java_enabled_indicator STRING,
java_script_version_code STRING,
--java_script_version_description STRING,
last_hit_time_gmt_unix STRING,
last_purchase_number STRING,
last_purchase_time_gmt STRING,
monthly_visitor_indicator STRING,
new_visit_indicator STRING,
operating_system_identifier STRING,
--operating_system_identifier_description STRING,
page_event_var1 STRING,
page_event_var2 STRING,
paid_search_indicator STRING,
persistent_cookie_indicator STRING,
post_browser_height STRING,
post_browser_width STRING,
post_channel STRING,
post_cookies_indicator STRING,
post_currency STRING,
post_cust_hit_time_gmt_unix STRING,
post_java_enabled_indicator STRING,
post_page_event_var1 STRING,
post_page_event_var2 STRING,
post_page_name_no_url STRING,
post_persistent_cookie_indicator STRING,
post_referrer STRING,
post_search_engine STRING,
post_t_time_info STRING,
post_visid_type_code STRING,
--post_visid_type_description STRING,
post_zip_code STRING,
quarterly_visitor_indicator STRING,
referral_type_code STRING,
--referral_type_description STRING,
referrer STRING,
resolution_identifier STRING,
--resolution_identifier_description STRING,
screen_resolution STRING,
sampled_hit_indicator STRING,
search_engine_identifier STRING,
--search_engine_identifier_description STRING,
search_page_number STRING,
secondary_hit STRING,
source_identifier STRING,
stats_server STRING,
visitor_time_information STRING,
truncated_hit_indicator STRING,
user_hash STRING,
value_closer_identifier STRING,
value_finder_identifier STRING,
value_instance_event STRING,
value_new_engagement STRING,
visid_new_indicator STRING,
visid_type_code STRING,
--visid_type_description STRING,
visit_referral_type_code STRING,
--visit_referral_type_description STRING,
visit_referrer STRING,
visit_search_engine STRING,
weekly_visitor_indicator STRING,
yearly_visitor_indicator STRING,
row_created_timestamp STRING,
row_created_by STRING,
data_owner_entity STRING, 
visit_start_datetime_gmt   TIMESTAMP,
first_hit_datetime_gmt     TIMESTAMP,
hit_datetime_gmt           TIMESTAMP,
last_hit_datetime_gmt      TIMESTAMP,
post_cust_hit_datetime_gmt TIMESTAMP,
cust_hit_datetime_gmt      TIMESTAMP,
visit_start_datetime       TIMESTAMP,
visit_end_datetime         TIMESTAMP,
total_visit_time_seconds   STRING,
Provider_List_View_Type    STRING, 
Provider_List_View_Type_ID STRING,  
product_list               STRING, 
typeahead_Search_Results_List STRING,
mobile_web STRING, 
OS_choice          STRING   , 
post_mobile_appid  STRING
)
PARTITIONED BY (
load_year string,
load_month string,
load_day string)
stored as orc tblproperties("orc.compress"="SNAPPY"); 


INSERT INTO TABLE ${hivevar:prod_conformed_db}.enriched_adobe_ah partition
       ( 
              load_year, 
              load_month , 
              load_day 
       ) 
SELECT  
--individual_identifier ,    
individual_analytics_identifier ,
individual_analytics_identifier_source,
interaction_type ,
interaction_source ,
preferred_proxy_identifier,
source_individual_identifier ,
source_alternate_identifier ,
source_subscriber_cumb_identifier ,
source_proxy_identifier ,
source_member_identifier,
first_name ,
last_name ,
email_address ,
zip_code ,
id_card_type ,
session_identifier ,
download_file_name ,
previous_page_name ,
page_name ,
form_name ,
page_url ,
mobile_identifier ,
dependent_indicator ,
marketing_cloud_visit_identifier ,
visit_number ,
hit_identifier_high ,
hit_identifier_low ,
visit_start_page_name ,
visit_page_number ,
visitor_identifier ,
hit_source ,
visit_start_time_gmt_unix ,
visid_high ,
visid_low ,
post_visid_high ,
post_visid_low ,
exclude_hit ,
post_prop2 ,
post_prop1 ,
user_visit_datetime ,
user_identifier ,
user_name ,
user_birth_date ,
user_gender_source ,
user_zip_code ,
user_email_address ,
user_authentication_status ,
user_login_timestamp ,
user_agent ,
login_sso_indicator ,
login_my_assistant ,
profile_ethnicity_code_source ,
--profile_ethnicity_code_description ,
--profile_primary_spoken_language_code_source ,
--profile_primary_spoken_language_description ,
--profile_primary_written_language_code_source ,
--profile_primary_written_language_description ,
post_event_list ,
event_list ,
page_event ,
post_page_event ,
post_product_list ,
post_page_name ,
post_page_url ,
interaction_generated_error_text ,
generic_link_name ,
pct_of_page_viewed ,
query_string ,
input_filter_name ,
input_filter_value ,
claim_name ,
automatic_login_indicator ,
source_language_code ,
--language_description ,
country_identifier_source ,
--country_description ,
state_abbreviation ,
channel_description ,
duplicate_purchase ,
email_identifier ,
email_campaign_identifier ,
registration_first_name ,
registration_last_name ,
search_results_count ,
registration_step_name ,
registration_id_type ,
registration_flow_type ,
input_doc_find_search_term ,
input_doc_find_distance ,
ol_find_what_looking_for_indicator ,
input_doc_find_zip_code ,
input_doc_find_type_of_search ,
qualtrics_respondent_identifier ,
ol_value_of_text_box ,
ol_value_of_ask ,
ol_able_to_register_login_indicator ,
ol_value_of_information ,
ol_overall_satisfaction,
ol_need_call_customer_service_indicator,
ol_ease_of_use,
ol_comments_left_indicator,
ol_appearance,
ngx_search_term,
ngx_search_result_identifier ,
ngx_search_result_category,
ngx_membership_identifier,
ngx_claim_detail_identifier,
mpe_distance ,
mpe_search_category ,
mpe_search_result_count ,
mpe_search_term ,
mpe_zip_code ,
mpe_member_name ,
mpe_facility_search_term ,
member_age_during_interaction ,
organization_identifier ,
organization_arrangement_identifier ,
plan_sponsor_identifier ,
plan_sponsor_name ,
--accept_language_description ,
browser_identifier ,
--browser_identifier_description ,
browser_height ,
browser_width ,
mobile_carrier ,
clickmap_click_action ,
clickmap_click_action_type ,
clickmap_click_context ,
clickmap_click_context_type ,
clickmap_click_source_identifier ,
clickmap_click_tag ,
code_version ,
color_identifier ,
--color_identifier_description ,
connection_type_identifier ,
--connection_type_identifier_description ,
cookies_indicator ,
currency_factor ,
currency_rate ,
cust_hit_time_gmt_unix ,
daily_visitor ,
user_domain ,
first_hit_page_url ,
first_hit_page_name ,
first_hit_referrer_type_code ,
--first_hit_referrer_type_description ,
first_hit_referrer ,
first_hit_time_gmt_unix ,
geo_city ,
geo_country ,
geo_designated_market_area ,
geo_region ,
geo_zip_code ,
hit_time_gmt_unix ,
home_page_indicator ,
new_hourly_visitor_indicator ,
ip_address ,
java_jscript_browser_version ,
java_enabled_indicator ,
java_script_version_code ,
--java_script_version_description ,
last_hit_time_gmt_unix ,
last_purchase_number ,
last_purchase_time_gmt ,
monthly_visitor_indicator ,
new_visit_indicator ,
operating_system_identifier ,
--operating_system_identifier_description ,
page_event_var1 ,
page_event_var2 ,
paid_search_indicator ,
persistent_cookie_indicator ,
post_browser_height ,
post_browser_width ,
post_channel ,
post_cookies_indicator ,
post_currency ,
post_cust_hit_time_gmt_unix ,
post_java_enabled_indicator ,
post_page_event_var1 ,
post_page_event_var2 ,
post_page_name_no_url ,
post_persistent_cookie_indicator ,
post_referrer ,
post_search_engine ,
post_t_time_info ,
post_visid_type_code ,
--post_visid_type_description ,
post_zip_code ,
quarterly_visitor_indicator ,
referral_type_code ,
--referral_type_description ,
referrer ,
resolution_identifier ,
--resolution_identifier_description ,
screen_resolution ,
sampled_hit_indicator ,
search_engine_identifier ,
--search_engine_identifier_description ,
search_page_number ,
secondary_hit ,
source_identifier ,
stats_server ,
visitor_time_information ,
truncated_hit_indicator ,
user_hash ,
value_closer_identifier ,
value_finder_identifier ,
value_instance_event ,
value_new_engagement ,
visid_new_indicator ,
visid_type_code ,
--visid_type_description ,
visit_referral_type_code ,
--visit_referral_type_description ,
visit_referrer ,
visit_search_engine ,
weekly_visitor_indicator ,
yearly_visitor_indicator ,
row_created_timestamp ,
row_created_by ,
data_owner_entity , 
visit_start_datetime_gmt,
first_hit_datetime_gmt ,
hit_datetime_gmt ,
last_hit_datetime_gmt ,
post_cust_hit_datetime_gmt ,
cust_hit_datetime_gmt ,
visit_start_datetime ,
visit_end_datetime ,
total_visit_time_seconds,
Provider_List_View_Type  , 
Provider_List_View_Type_ID  ,  
product_list   ,  
typeahead_Search_Results_List ,
mobile_web, 
OS_choice,
post_mobile_appid  , 
load_year, 
load_month , 
load_day  
FROM   ${hivevar:prod_conformed_db}.stg_enriched_adobe_ah ;

--WHERE  load_year=year(CURRENT_DATE) AND load_month=month(CURRENT_DATE) AND load_day=day(CURRENT_DATE);



-- Clean up all temporary tables
 
  DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.persona_proxy_ah purge;
  DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.enriched_session_2 purge;   
  DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.persona_proxy_for_enriched_session_3 purge;   
  DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.persona_proxy_for_enriched_session_3a  purge;
  DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.tmp_enriched_clickstream_interaction_10 purge;
  --DROP TABLE IF EXISTS ${hivevar:prod_conformed_db}.stg_conformed_adobe_ah PURGE;
  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.enriched_adobe_ngx_1 purge ;
  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.enriched_adobe_ngx_1a purge ;
  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.enriched_adobe_ngx_2a purge ;
  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.enriched_adobe_ngx_2 purge ;
  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.tmp_enriched_ngx_interaction_10 PURGE;
  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.enriched_adobe_ngx_tmp purge;
  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.enriched_adobe_ngx_tmp_1 purge;

  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.stg_conformed_adobe_ah purge;
  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.stg_enriched_adobe_ah purge;
  DROP  TABLE IF EXISTS ${hivevar:prod_conformed_db}.stg_enriched_adobe_ngx purge;



