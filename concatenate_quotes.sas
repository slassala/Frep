%macro drop_lead_ddl(leadsTableName=);
%let existe_tab = NoExiste;

proc sql noprint;
     connect to oracle (PATH=&orapath. USER=&cdm_user. PASS="&cdm_pass.");
	 select TABLE_NAME into: existe_tab
	 from connection to oracle
		(
		SELECT upper(TABLE_NAME) as table_name
		FROM USER_TABLES 
		WHERE upper(TABLE_NAME)= %upcase(%str(%')&leadsTableName.%str(%')) 
		);
	 
%if &existe_tab eq %upcase(&leadsTableName) %then %do;	
	execute(DROP TABLE &cdm_schema..&leadsTableName.)by oracle;
	%put The table %lowcase(&cdm_schema..&leadsTableName.) has been deleted;
%end;
%else %put There is not table to drop;
%mend drop_lead_ddl;