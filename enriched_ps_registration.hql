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
--set hivevar:prod_conformed_db=prod_insights_conformed_enc;
--set prod_conformed_db;
--set hivevar:prod_enriched_db=BI_NGX_PROD_ENC;
--set prod_enriched_db;
--set hivevar:adobe_db=ADOBE_ENC;
--set adobe_db;
--set hivevar:dev_insights=dev_insights_pilot_enc;
--set dev_insights;
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


 
drop table ${hivevar:prod_enriched_db}.enriched_ps_active_registration_bkp;

create table ${hivevar:prod_enriched_db}.enriched_ps_active_registration_bkp  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
As   
SELECT * from ${hivevar:prod_enriched_db}.enriched_ps_active_registration  ; 


-- (case when em.customer_group_identifier in ('000002','000003','000005')
--         and pp.individual_analytics_id_source = 'HRP'
--         then -1
-- Individual Medicare

--SELECT count(1), customer_group_identifier FROM prod_insights_conformed_enc.enriched_membership 
--where source='HRP' 
--group by customer_group_identifier;
--000001
--000002
--000003
--000005


--Example:
--MEA (Aetna Commerical membership)
--HMO(Aetna Medicare Membership (which can be identified by the first 2 bytes of individual_analytics_identifier='ME')), 
--HRP (Consumer Business, like individual medicare membership)
--Coventry Medicare membership -- 1010598465
 
--select count(1), source FROM prod_insights_conformed_enc.enriched_membership 
--group by source;
--HRP
--CVH
--HMO
--MEA

--SELECT count(1), business_ln_cd FROM edw_enc.edw_emis_membership group by business_ln_cd  
--CP
--ME

----Seperate out the records by search_result  CHECK FOR DUPLICATES ----------------------------------------------------------------------


drop table ${hivevar:prod_enriched_db}.get_all_dep_recs ;

create table ${hivevar:prod_enriched_db}.get_all_dep_recs  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS
select mem_dep.memberindividualid individual_id,  mem_dep.anregdt dependent_reg_date , 'Dependent' member_type , mem_dep.subscriberindividualid sub_individual_id  
from  
(select memberindividualid, anregdt,  subscriberindividualid , row_number() over (partition BY memberindividualid   ORDER BY anregdt desc) rank1 
from ${hivevar:dev_insights}.member_dependent) as mem_dep   
where   mem_dep.rank1=1; 




drop table ${hivevar:prod_enriched_db}.get_all_mem_recs;

create table ${hivevar:prod_enriched_db}.get_all_mem_recs    
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
select mem.individualid individual_id,   mem.anregdt mem_reg_date  ,   b.sub_individual_id , 'member' member_type 
from  
(select  individualid, anregdt,  row_number() over (partition BY individualid   ORDER BY  anregdt desc) rank1
from ${hivevar:dev_insights}.member) as mem    
left outer join ${hivevar:prod_enriched_db}.get_all_dep_recs B 
ON 
B.individual_id=mem.individualid  
where   mem.rank1=1;



--drop table ${hivevar:prod_enriched_db}.get_all_mem_dep_recs;

--create table ${hivevar:prod_enriched_db}.get_all_mem_dep_recs    
--STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
--AS
--select individual_id, mem_reg_date reg_date ,   sub_individual_id subscriber_individual_id,   
--case when sub_individual_id is not null    then 'Dependent'  
--     else 'Subscriber' end as member_type  
--from  
--${hivevar:prod_enriched_db}.get_all_mem_recs ;     



drop table ${hivevar:prod_enriched_db}.get_all_mem_dep_recs ;

create table ${hivevar:prod_enriched_db}.get_all_mem_dep_recs  
STORED AS ORC tblproperties("orc.compress"="SNAPPY")  
AS
select  individual_id,   mem_reg_date reg_date,  member_type from ${hivevar:prod_enriched_db}.get_all_mem_recs 
UNION
select  individual_id,   dependent_reg_date reg_date,   member_type from ${hivevar:prod_enriched_db}.get_all_dep_recs;




--------------------- Get all other attributes from enriched_membership table -----------------------------------------------------




drop table ${hivevar:prod_enriched_db}.get_all_base_reg_enriched_sub ;

create table ${hivevar:prod_enriched_db}.get_all_base_reg_enriched_sub  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS
select * from  
(SELECT insights_posted_timestamp date_time, individual_id, proxy_id, 
individual_analytics_identifier, source , individual_analytics_subscriber_identifier, relationship_to_subscriber_description , 
business_effective_date, business_expiration_date, coverage_expiration_date, 
ps_unique_id, customer_name ps_name, customer_group_identifier  control_number , segment_code, segment_name, customer_sub_segment_code sub_segment_code,  sub_segment_name, 
org_id, org_name, org_arrangement_id , funding_arrangement_code_source funding_arrangement_code, funding_arrangement_code_description, funding_category funding_category_code, funding_category_description,  
row_number() over (partition BY  proxy_id  ORDER BY business_effective_date desc, business_expiration_date desc) rank1  
FROM ${hivevar:prod_conformed_db}.enriched_membership)  em  
Where trim(em.individual_analytics_identifier) = trim(em.individual_analytics_subscriber_identifier) 
  and trim(em.individual_analytics_subscriber_identifier) !=''  
  and em.business_effective_date <= current_date 
  and em.coverage_expiration_date > current_date
  and em.business_expiration_date > current_date ;
 -- and em.rank1 = 1  ;



drop table ${hivevar:prod_enriched_db}.get_all_base_reg_enriched_mem ;

create table ${hivevar:prod_enriched_db}.get_all_base_reg_enriched_mem  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS
select * from  
(SELECT insights_posted_timestamp date_time, individual_id, proxy_id, 
individual_analytics_identifier, source , individual_analytics_subscriber_identifier, relationship_to_subscriber_description , 
business_effective_date, business_expiration_date, coverage_expiration_date, 
ps_unique_id, customer_name ps_name, customer_group_identifier  control_number , segment_code, segment_name, customer_sub_segment_code sub_segment_code,  sub_segment_name, 
org_id, org_name, org_arrangement_id , funding_arrangement_code_source funding_arrangement_code, funding_arrangement_code_description, funding_category funding_category_code, funding_category_description,  
row_number() over (partition BY  proxy_id  ORDER BY business_effective_date desc, business_expiration_date desc) rank1  
FROM ${hivevar:prod_conformed_db}.enriched_membership)  em  
Where 
      em.business_effective_date <= current_date 
  and em.coverage_expiration_date > current_date
  and em.business_expiration_date > current_date ;



--------------------- get Subscriber details -----------------------------------------------------




drop table ${hivevar:prod_enriched_db}.get_subscriber_reg_details_A ;

create table ${hivevar:prod_enriched_db}.get_subscriber_reg_details_A   
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
SELECT    
cast(mem.individual_id as string) individual_id, 
mem.mem_reg_date reg_date ,  
'Subscriber' member_type ,
em.proxy_id ,   
min(em.rank1) rank2 
--em.individual_analytics_identifier 
--em.source , 
--em.individual_analytics_subscriber_identifier, 
--em.relationship_to_subscriber_description  
FROM 
${hivevar:prod_enriched_db}.get_all_mem_recs   mem  
LEFT OUTER JOIN 
${hivevar:prod_enriched_db}.get_all_base_reg_enriched_sub em 
ON mem.individual_id = em.individual_id AND 
trim(em.individual_analytics_identifier) = trim(em.individual_analytics_subscriber_identifier) 
and trim(em.individual_analytics_subscriber_identifier) !=''  
group by 
mem.individual_id, 
mem.mem_reg_date ,  
mem.member_type ,
em.proxy_id  
--em.individual_analytics_identifier  
--em.source , 
--em.individual_analytics_subscriber_identifier, 
--em.relationship_to_subscriber_description  
 ; 

 

drop table ${hivevar:prod_enriched_db}.get_subscriber_reg_details ;

create table ${hivevar:prod_enriched_db}.get_subscriber_reg_details   
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
SELECT    DISTINCT 
cast(mem.individual_id as string) individual_id, 
reg_date ,  
member_type ,
em.proxy_id ,   
mem.rank2 ,
em.individual_analytics_identifier, 
em.source , 
em.individual_analytics_subscriber_identifier, 
em.relationship_to_subscriber_description , 
em.business_effective_date, 
em.business_expiration_date, 
em.coverage_expiration_date, 
em.ps_unique_id, 
em.ps_name, 
em.control_number , 
em.segment_code, 
em.segment_name, 
em.sub_segment_code,  
em.sub_segment_name, 
em.org_id, em.org_name, 
em.org_arrangement_id , 
em.funding_arrangement_code, 
em.funding_arrangement_code_description, 
em.funding_category_code, 
em.funding_category_description 
FROM 
${hivevar:prod_enriched_db}.get_subscriber_reg_details_A  mem  
LEFT OUTER JOIN 
${hivevar:prod_enriched_db}.get_all_base_reg_enriched_sub em 
ON mem.individual_id = em.individual_id AND 
em.rank1 = mem.rank2  ;

 


---------------------  get dependent details -----------------------------------------------------



drop table ${hivevar:prod_enriched_db}.get_dependent_reg_details_A ;

create table ${hivevar:prod_enriched_db}.get_dependent_reg_details_A   
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
SELECT    
cast(em.individual_id as string) individual_id,   
em.proxy_id ,   
min(em.rank1) rank2  
FROM  
${hivevar:prod_enriched_db}.get_all_base_reg_enriched_mem em  
group by 
em.individual_id,   
em.proxy_id   
; 


drop table ${hivevar:prod_enriched_db}.get_dependent_reg_details_B ;

create table ${hivevar:prod_enriched_db}.get_dependent_reg_details_B  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
SELECT     DISTINCT 
em.individual_id,  
em.proxy_id , 
dep.rank2, 
em.individual_analytics_identifier, 
em.source , 
em.individual_analytics_subscriber_identifier, 
em.relationship_to_subscriber_description , 
em.business_effective_date, 
em.business_expiration_date, 
em.coverage_expiration_date, 
em.ps_unique_id, 
em.ps_name, 
em.control_number , 
em.segment_code, 
em.segment_name, 
em.sub_segment_code,  
em.sub_segment_name, 
em.org_id, em.org_name, 
em.org_arrangement_id , 
em.funding_arrangement_code, 
em.funding_arrangement_code_description, 
em.funding_category_code, 
em.funding_category_description 
FROM  
${hivevar:prod_enriched_db}.get_dependent_reg_details_A  dep
LEFT OUTER JOIN 
${hivevar:prod_enriched_db}.get_all_base_reg_enriched_mem em    
ON  trim(em.individual_id) = trim(dep.individual_id)  
AND trim(em.individual_analytics_identifier) != trim(em.individual_analytics_subscriber_identifier)  
and trim(em.individual_analytics_subscriber_identifier) !=''
and em.rank1 = dep.rank2  
; 



drop table ${hivevar:prod_enriched_db}.get_dependent_reg_details;

create table ${hivevar:prod_enriched_db}.get_dependent_reg_details    
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
SELECT     
dep.individual_id, 
sub.reg_date reg_date,  
'Dependent' member_type ,
dep.proxy_id , 
dep.rank2, 
dep.individual_analytics_identifier, 
dep.source , 
dep.individual_analytics_subscriber_identifier, 
dep.relationship_to_subscriber_description , 
dep.business_effective_date, 
dep.business_expiration_date, 
dep.coverage_expiration_date, 
dep.ps_unique_id, 
dep.ps_name, 
dep.control_number , 
dep.segment_code, 
dep.segment_name, 
dep.sub_segment_code,  
dep.sub_segment_name, 
dep.org_id, 
dep.org_name, 
dep.org_arrangement_id , 
dep.funding_arrangement_code, 
dep.funding_arrangement_code_description, 
dep.funding_category_code, 
dep.funding_category_description 
FROM 
${hivevar:prod_enriched_db}.get_dependent_reg_details_B  dep 
LEFT OUTER JOIN 
${hivevar:prod_enriched_db}.get_subscriber_reg_details   sub   
ON trim(sub.individual_analytics_identifier) = trim(dep.individual_analytics_subscriber_identifier)  
WHERE 
trim(dep.individual_analytics_identifier) != trim(dep.individual_analytics_subscriber_identifier) 
and trim(dep.individual_analytics_subscriber_identifier) !=''  
; 
 
 

 

drop table ${hivevar:prod_enriched_db}.get_member_reg_details;

create table ${hivevar:prod_enriched_db}.get_member_reg_details   
STORED AS ORC tblproperties("orc.compress"="SNAPPY")  
AS
select  * from ${hivevar:prod_enriched_db}.get_subscriber_reg_details 
UNION
select  * from ${hivevar:prod_enriched_db}.get_dependent_reg_details;




--------------------- End dependent details -----------------------------------------------------



--3456276  are members from SQL db which either do not have a match with enriched_membership as iai is null, so not sure if they are subscribers or members
-- OR      are members from SQL db which either have a match with enriched_membership as iai is not null, but appear to be dependents in enriched_membership table
--count	      member_type
--410	      Partner-member
--296	      U-member
--2361	      Child-member
--584487      dependent
--25198	      Spouse-member
--42	      Self-member
--5338400     subscriber
--3427963     member
--7	      Sponsored-member

--680 cases where ps_name is TEST
--Registration source data has basically demographics data ; it has membership data just like edw ; this does not mean that all the recs should tie to visits table; that shoudl be in sync with NGX 
--More chances of tieing with the visits table using ps_unique_id than indiviual_id  


-- How many dependents in dependent table match up with members in member table
-- 13,625
--counts of members
--SELECT count(1) FROM bi_ngx_prod_enc.get_all_mem_recs;
--8,794,677

--count of dependents
--SELECT count(1) FROM bi_ngx_prod_enc.get_all_dep_recs ;
--584,487



---------------------Get Organization details and unique individual ids -----------------------------------------------------



drop table ${hivevar:prod_enriched_db}.get_all_base_reg_recs ;

create table ${hivevar:prod_enriched_db}.get_all_base_reg_recs  
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS 
SELECT    
mem.individual_id, 
mem.reg_date reg_date,   
mem.member_type   ,
mem.proxy_id ,  
mem.rank2 ,
mem.individual_analytics_identifier, 
mem.source , 
mem.individual_analytics_subscriber_identifier, 
mem.relationship_to_subscriber_description , 
mem.business_effective_date, 
mem.business_expiration_date, 
mem.coverage_expiration_date, 
mem.ps_unique_id, 
mem.ps_name, 
mem.control_number , 
mem.segment_code, 
mem.segment_name, 
mem.sub_segment_code,  
mem.sub_segment_name, 
mem.org_id,
mem.org_name, 
mem.org_arrangement_id , 
mem.funding_arrangement_code, 
mem.funding_arrangement_code_description funding_arrangement_description, 
mem.funding_category_code, 
mem.funding_category_description ,  
b.arr_nm org_arrangement_name, 
row_number() over (partition BY individual_id  ORDER BY reg_date desc, business_effective_date desc) rank1    
FROM  
${hivevar:prod_enriched_db}.get_member_reg_details  mem   
LEFT OUTER JOIN
(select org_id, arr_id, arr_nm , arr_eff_dt, row_number() over (partition by org_id, arr_id order by arr_eff_dt desc) as rownbr from p4p_enc.obor_arr) b 
ON 
b.org_id = mem.org_id  and
b.arr_id = mem.org_arrangement_id  and 
b.rownbr=1 ;
 




----Filter Active rows we need from base table for member registration details ----------------------------------------------------------------------
--If subscriber_reg_date is null..means that individual_id does not exist in Shane's SQL database
--If org_name is null..org_id is null
--If individualid is null..then proxy_id is null and does not exist in persona_proxy table 
--If individual_analytics_identifier is blank it means that the member is termed.
--select count(1), proxy_id, individual_analytics_identifier, source  
--from bi_ngx_prod_enc.enriched_ps_active_registration 
--group by proxy_id, individual_analytics_identifier, source 
--having count(1) > 1;
--Ranking by business_effective_date desc ranking rownum=1
 



--------------------- Create Active Registration data for UDF -----------------------------------------------------


drop table ${hivevar:prod_enriched_db}.enriched_ps_all_registration_hdp;

create table ${hivevar:prod_enriched_db}.enriched_ps_all_registration_hdp    
STORED AS ORC tblproperties("orc.compress"="SNAPPY") 
AS
SELECT   * , 
current_timestamp      row_created_timestamp,
'${hivevar:row_created_by}'    row_created_by 
from ${hivevar:prod_enriched_db}.get_all_base_reg_recs where reg_date is not null;




-- create and fill the table for sqoop to netezza for Active membership 
 
 

DROP TABLE IF EXISTS ${hivevar:prod_enriched_db}.enriched_ps_active_registration purge;
CREATE  TABLE ${hivevar:prod_enriched_db}.enriched_ps_active_registration        
( 
individual_id string,  
proxy_id string,   
individual_analytics_identifier string, 
source string, 
individual_analytics_subscriber_identifier string, 
relationship_to_subscriber_description string, 
reg_date date, 
business_expiration_date date,  
coverage_expiration_date date, 
member_type string, 
ps_unique_id decimal(16,0),
ps_name string,
control_number string,
segment_code string,
segment_name string,
sub_segment_code string,
sub_segment_name string,
org_id string,
org_name string,
org_arrangement_id string,
org_arrangement_name string,
funding_arrangement_code string,
funding_arrangement_description string,
funding_category_code string,
funding_category_description string, 
row_created_timestamp timestamp, 
row_created_by string  
)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile; 



--INSERT  DATA 
--IF REG_DATE IS NULL MEANING IT IS NOT PART OF THE REGISTERED SUBSCRIBERS
insert into ${hivevar:prod_enriched_db}.enriched_ps_active_registration  
select  DISTINCT 
cast(individual_id as string) individual_id, 
proxy_id ,   
individual_analytics_identifier , 
source , 
individual_analytics_subscriber_identifier , 
relationship_to_subscriber_description , 
reg_date,
business_expiration_date, 
coverage_expiration_date, 
member_type,    
ps_unique_id, 
ps_name,
control_number,
segment_code,
segment_name,
sub_segment_code,
sub_segment_name,
cast(org_id as string) org_id,
org_name,
cast(org_arrangement_id as string) org_arrangement_id,
org_arrangement_name,
funding_arrangement_code,
funding_arrangement_description,
funding_category_code,
funding_category_description,
row_created_timestamp , 
row_created_by   
from  ${hivevar:prod_enriched_db}.enriched_ps_all_registration_hdp 
where reg_date is not null 
and business_expiration_date > current_date and coverage_expiration_date > current_date 
and individual_analytics_identifier is not null 
and (trim(ps_name) is null or (trim(ps_name) is not null and trim(ps_name) not like '%*TEST*%'));
 


---- temporary tables ----------------------------------------------------------------------


 

drop table IF EXISTS ${hivevar:prod_enriched_db}.get_all_sub_recs purge;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_all_dep_recs purge;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_all_mem_dep_recs purge;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_proxy_id purge;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_all_base_reg_enriched_mem purge;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_all_base_reg_recs purge;

drop table IF EXISTS ${hivevar:prod_enriched_db}.get_all_mem_recs purge;

drop table IF EXISTS ${hivevar:prod_enriched_db}.enriched_ps_registration_1 purge;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_member_reg_details purge;

drop table IF EXISTS ${hivevar:prod_enriched_db}.get_subscriber_reg_details purge;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_dependent_reg_details purge;

drop table IF EXISTS ${hivevar:prod_enriched_db}.get_all_base_reg_enriched_sub purge ;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_dependent_reg_details_a purge;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_dependent_reg_details_b purge;
drop table IF EXISTS ${hivevar:prod_enriched_db}.get_subscriber_reg_details_a purge;
