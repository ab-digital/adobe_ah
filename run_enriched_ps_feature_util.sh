
#!/bin/sh

#create Feature utlization dataset
/bin/ksh /u01/datascience/ab_digital/adobe_ah/scripts/enriched_ps_feature_util.sh bi_ngx_prod_enc adobe_enc S018143 prod_insights_conformed_enc

#Push Feature utlization dataset to Netezza 
#sh /u01/datascience/ab_digital/adobe_ah/scripts/ps_feature_util_sqoop.sh


