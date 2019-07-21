
#!/bin/sh

#create Active Registration dataset  
/bin/ksh /u01/datascience/ab_digital/adobe_ah/scripts/enriched_ps_registration.sh bi_ngx_prod_enc adobe_enc S018143 prod_insights_conformed_enc dev_insights_pilot_enc 


#Push Active Registration dataset to Netezza 
#sh /u01/datascience/ab_digital/adobe_ah/scripts/ps_active_registration_sqoop.sh 
