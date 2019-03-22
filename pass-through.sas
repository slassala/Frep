PROC SQL;
connect to oracle (user=&USUARIO_DATAPOOL orapw=&PSSWRD_DATAPOOL path='DPMKTBCN' PRESERVE_TAB_NAMES=YES PRESERVE_COL_NAMES=YES READBUFF=5000);
LIBNAME TSB_CDM ORACLE  PATH=CDM01ACE  SCHEMA=U_SAS_ADMIN  USER=SAS_DB_USER  PASSWORD="{SAS002}8083755C0CE9998849AE51983BD02BE1" ;

PROC SQL;
connect to oracle (user=SAS_DB_USER orapw="{SAS002}8083755C0CE9998849AE51983BD02BE1"  path=CDM01ACE );
select * from connection to oracle
  (select  
	priority,
	Lead_Treatment_Id,
	Party_id,
	default_PRIORITY,
	lead_family,
	channel_cd,
	data_token,
	lead_type_code,
	communication_cd,
	opportunity_name,
	outcome,
	entity,
	status_desc,
	brand_code,
	secundary_outcome,
	lead_description_line,
	expiration_date,
	creation_date,
	id_campaign,
	categorisation,
	creativity_id,
	secundary_outcome_desc,
	outcome_desc  
  from(
   select 
	l.priority,
	l.Lead_Treatment_Id,
	l.Party_id,
	l.default_PRIORITY,
	l.lead_family,
	l.channel_cd,
	l.data_token,
	l.lead_type_code,
	l.communication_cd,
	l.opportunity_name,
	l.outcome,
	l.entity,
	l.status_desc,
	l.brand_code,
	l.secundary_outcome,
	l.lead_description_line,
	l.expiration_date,
	l.creation_date,
	l.id_campaign,
	l.categorisation,
	l.creativity_id,
	l.secundary_outcome_desc,
	l.outcome_desc,    
       ROW_NUMBER() OVER (PARTITION BY l.party_id ORDER BY l.priority DESC) as rn 
      from u_sas_admin.ci_leads_enri l
      where area='873401' and 
      network='873401' and 
      expiration_date>sysdate and 
      customer_opportunity='2' and 
      sort_code='873401' and 
      channel_cd='BRN' and 
      available_flag=1 and 
      opportunity_type='22' and 
      customer_type='0'
      order by party_id, priority desc)
      where rn=1 order by priority DESC);             
          
 