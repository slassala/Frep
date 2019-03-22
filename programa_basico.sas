
/* Adds & Retracts */

%include "/DATOS/sascode/auto/autoexec_dp.sas";
%include "/DATOS/sascode/auto/autoexec_batch.sas";


/* Exec subprocess */
%exec_subprocess_start(sonProc=ADDS_RETRACTS);

%include "/DATOS/sascode/code/functions.sas";

%global maxLead;

/* Create empty table SCPC.aux_records_leads */
data &scpc_lib..aux_records_leads;
	length fichero $256 registros 8;
        stop;
run;

/* Copy SCPC.records_leads to SCPC.records_leads_previous */
%let rl_vacio=1;

%macro crea_previous(tabla);
        %if not %sysfunc(exist(&tabla.)) %then %do;
                data &scpc_lib..records_leads;
                        stop;
                run;
        %end;
		%else %do;
			data &scpc_lib..records_leads;
				set &scpc_lib..records_leads;
				if upcase(substr(compress(CAMPAIGN_CD),1,3)) ne 'SGL';
            run;
		%end;

	data _null_;
                set &tabla;
                call symput('rl_vacio','0');
        run;
        %if &rl_vacio %then %do;
		data &scpc_lib..records_leads_previous;
			length CAMPAIGN_CD $30 COMMUNICATION_CD $30 CHANNEL $60 SLR_TODAY 8 SLR_PREVIOUS 8 ADDITION 8 RETRACTION 8;
			stop;
		run;
	%end;
	%if &rl_vacio=0 %then %do;
		proc datasets library=&scpc_lib.;
			delete records_leads_previous;
		quit;
		data &scpc_lib..records_leads_previous;
			set &scpc_lib..records_leads;
		run;
	%end;
%mend;
%crea_previous(&scpc_lib..records_leads);

/* RetracFile location */
%let file_retracted = /DATOS/sastransfer/crmout/&day_exec_yyyymmdd._RetractionRules.csv;

/* Get start and end date to DP04 for get the tables to execute*/
%let dp_exec_campaigns = DIRECTOR_PROCESS_04;

/* Initialize Leads Tables */
%generate_leads_table;

/* Generate control table for leads channels */
PROC SQL;
   CREATE TABLE sin_channel AS 
   SELECT t2.CAMPAIGN_CD, 
          t2.COMMUNICATION_CD, 
          t1.registros
   FROM &scpc_lib..AUX_RECORDS_LEADS t1 INNER JOIN &scpc_lib..LEADS_TABLES t2 ON (upcase(t1.fichero) = upcase(t2.LEADTABLE));
QUIT;

/* Obtain channel_cd */
PROC SQL;
   CREATE TABLE con_channel_cd AS 
   SELECT t1.CAMPAIGN_CD, 
          t1.COMMUNICATION_CD,
	  t2.CHANNEL_CD,
          t1.registros AS SLR_TODAY,
	  t2.PROCESSED_DTTM,
	  max(t2.PROCESSED_DTTM) AS MAXIMO
      FROM sin_channel t1 INNER JOIN CDM_TSB.CI_REFERENCE_DATA t2
      ON (upcase(t1.CAMPAIGN_CD) = upcase(t2.CAMPAIGN_CD) AND upcase(t1.COMMUNICATION_CD) = upcase(t2.COMMUNICATION_CD) AND upcase(t2.LEAD_KIND) = 'CNL')
           GROUP BY t1.CAMPAIGN_CD, t1.COMMUNICATION_CD;
QUIT;

data sin_channel_nm;
        set con_channel_cd;
        if PROCESSED_DTTM=MAXIMO;
        drop PROCESSED_DTTM MAXIMO;
run;

/* Obtain channel_nm */
PROC SQL;
   CREATE TABLE con_channel_nm AS
   SELECT t1.CAMPAIGN_CD,
	  t1.COMMUNICATION_CD,
	  t2.CHANNEL_NM AS CHANNEL,
	  t1.SLR_TODAY
      FROM sin_channel_nm t1 INNER JOIN CDM_TSB.CI_CHANNEL t2 ON (t1.CHANNEL_CD = t2.CHANNEL_CD);
QUIT;

PROC SQL;
   CREATE TABLE records_leads AS
   SELECT coalesce(t1.CAMPAIGN_CD,t2.CAMPAIGN_CD) AS CAMPAIGN_CD,
	  coalesce(t1.COMMUNICATION_CD,t2.COMMUNICATION_CD) AS COMMUNICATION_CD,
          coalesce(t1.CHANNEL,t2.CHANNEL) AS CHANNEL,
          t1.SLR_TODAY,
	  (t2.SLR_TODAY) AS SLR_PREVIOUS
      FROM con_channel_nm t1 FULL JOIN &scpc_lib..records_leads_previous t2 ON (t1.CAMPAIGN_CD = t2.CAMPAIGN_CD AND t1.CHANNEL = t2.CHANNEL);
QUIT;
/* End generate control table for leads channels */

/* Get old leads with reference_data fields */

data CI_LEADS_OLD_CNL CI_LEADS_OLD_SGL;
set cdm_tsb.ci_leads_enri; 
if lead_kind = 'SGL' then output CI_LEADS_OLD_SGL;
if lead_kind = 'CNL' then output CI_LEADS_OLD_CNL;
run; 

proc sort data=CI_LEADS_OLD_CNL nodupkey;
        by ID_COMMUNICATION ID_CAMPAIGN PARTY_ID ID_CELL AREA NETWORK SORT_CODE;
run;

proc sort data=CI_LEADS_OLD_SGL;
        by ID_COMMUNICATION ID_CAMPAIGN PARTY_ID ID_CELL AREA NETWORK SORT_CODE;
run;

data CI_LEADS_OLD_ok;
set ci_leads_old_cnl ci_leads_old_sgl;
run;
	
/* Merge the tables */
data leads_merged leads_retracted;
  		merge LEADS_TODAY (in=tod) CI_LEADS_OLD (in=old);
		by ID_COMMUNICATION ID_CAMPAIGN PARTY_ID ID_CELL AREA NETWORK SORT_CODE;
		retain ID_NEW_LEAD;

		format STATUS_ADDR $20.;
		/* If first execution, initialize new lead */
		if _n_ = 1 then do;
                        ID_NEW_LEAD = &maxLead;
    		end;

		/* New generated leads */
		if tod and not old then do;
			STATUS_ADDR="AR_NEW";
			STATUS = STATUS_CD;
			HISTORY=0;
			ID_NEW_LEAD = ID_NEW_LEAD+1;
			ID_LEAD = ID_NEW_LEAD;
			CREATION_DATE=dhms(&day_exec.,0,0,0);
	                format CREATION_DATE datetime20.;
			IND_SGL=0;
			ASSIGNED_DATE = .;

			call symput("maxLead", ID_NEW_LEAD);

			output leads_merged;
		end;
		else if tod and old then do; 
			/* Lead exists, and must be updated. Creation_Date is not updated. */
			ID_LEAD = LEAD_TREATMENT_ID;
			CREATION_DATE = CREATION_DATE_L_OLD;
                        ASSIGNED_USER = ASSIGNED_USER_L_OLD;
                        UPDATE_USER = UPDATE_USER_L_OLD;
                        EXPIRY_DATE = EXPIRY_DATE_L_OLD;
                        LEAD_CR = LEAD_CR_L_OLD;
                        CHANNEL_CR = CHANNEL_CR_L_OLD;
			HISTORY=0;
			STATUS = STATUS_CD_L_OLD;
			IND_SGL = IND_SGL_L_OLD;
			ASSIGNED_DATE = ASSIGNED_DATE_L_OLD;

			/* Check if export to retraction file */			
			if (channel_cd = 'BRN' and status in (210)) then do;
                                export=1;
			end;
                        else do;
                        	export=0;
			end;

			/* Erased leads by Expiration_Date and Expiry_Date */
			if (expiry_date ne . and datepart(expiry_date)<=&day_exec.)
                        or (
				(missing(assigned_user) or assigned_user='0') 
				and assigned_date ne . and intnx('day',datepart(assigned_date),&days_unassigned.) <=&day_exec. 
				and lead_kind = 'CNL' and STATUS = 210
			)
                        then do;
                                STATUS_ADDR="AR_EXPIRY_D";
                                outcome=940;
                                secundary_outcome=9999;
                                STATUS=340;
				HISTORY=1;
                                output leads_retracted;
                        end;
			else if (expiration_date ne . and datepart(expiration_date)<=&day_exec.) 
			then do;		
				STATUS_ADDR="AR_EXPIRATION_D";
			       	outcome=950;
				secundary_outcome=9999;		
				STATUS=350; 
				HISTORY=1;
				output leads_retracted;
			end;
			else do;
				STATUS_ADDR="AR_MERGED";
				output leads_merged;
			end;
		end;
		else if not tod and old then do;
			/* Lead exists, but is not generated or updated */
			ID_LEAD = LEAD_TREATMENT_ID;

			PRIORITY = PRIORITY_L_OLD;
			STATUS_CD = STATUS_CD_L_OLD;
			STATUS = STATUS_CD_L_OLD;
			HISTORY=0;
			OUTCOME = OUTCOME_L_OLD;
			SECUNDARY_OUTCOME = SECUNDARY_OUTCOME_L_OLD;
			CREATION_DATE = CREATION_DATE_L_OLD;
			DATA_TOKEN = DATA_TOKEN_L_OLD;
			ASSIGNED_USER = ASSIGNED_USER_L_OLD; 
			UPDATE_USER = UPDATE_USER_L_OLD;
			EXPIRY_DATE = EXPIRY_DATE_L_OLD;
			LEAD_CR = LEAD_CR_L_OLD;
			CHANNEL_CR = CHANNEL_CR_L_OLD;
			IND_SGL = IND_SGL_L_OLD;
                        ASSIGNED_DATE = ASSIGNED_DATE_L_OLD;
			
			/* Check if export to retraction file */
                        if (channel_cd = 'BRN' and status in (210)) then do;
                                export=1;
                        end;
                        else do;
                                export=0;
                        end;
			
			/* Erased leads by Expiration_Date and Expiry_Date */
			if (expiry_date ne . and datepart(expiry_date)<=&day_exec.)
                        or (
				(missing(assigned_user) or assigned_user='0')
                                and assigned_date ne . and intnx('day',datepart(assigned_date),&days_unassigned.) <=&day_exec.
                                and lead_kind = 'CNL' and STATUS = 210
			)
                        then do;
                                STATUS_ADDR="AR_EXPIRY_D";
                                outcome=940;
                                secundary_outcome=9999;
                                STATUS = 340;
                                HISTORY=1;
                                output leads_retracted;
                        end;
                        else if (expiration_date ne . and datepart(expiration_date)<=&day_exec.)
                        then do;
	
				STATUS_ADDR="AR_EXPIRATION_D";
		    		outcome=950;
				secundary_outcome=9999;
				STATUS=350; 
				HISTORY=1;
				output leads_retracted;
			end;
			else do;
				if (STATUS_CD in (210) | lead_kind ne 'CNL') then do;
                                        STATUS_ADDR="AR_OLD_DYN";
                                        HISTORY = 0;
                                        output leads_merged;
                                end;
                                else do;
                                        STATUS_ADDR="AR_DEL_OLD";
                                        outcome=950;
                                        secundary_outcome=9999;
                                        STATUS=350;
                                        HISTORY = 1;
                                        output leads_retracted;
                                end;
			end;
		end;
		
		keep ID_LEAD PARTY_ID ID_CAMPAIGN ID_COMMUNICATION ID_CELL PRIORITY 
			STATUS OUTCOME SECUNDARY_OUTCOME CREATION_DATE DATA_TOKEN 
			ASSIGNED_USER UPDATE_USER AREA NETWORK SORT_CODE EXPIRY_DATE 
			LEAD_CR CHANNEL_CR IND_SGL ASSIGNED_DATE CHANNEL_CD EXPIRATION_DATE LEAD_KIND
			HISTORY EXPORT STATUS_ADDR 
			;
run;


/* Delete tables after merge them */
proc delete data=CI_LEADS_OLD;
run;
%error_batch;

proc delete data=LEADS_TODAY;
run;
%error_batch;	

/* Adjust values into leads history*/
data leads_history (drop= HISTORY);
	set leads_merged (drop= 
		IND_SGL ASSIGNED_DATE CHANNEL_CD EXPIRATION_DATE LEAD_KIND EXPORT );
	where history=1;
run;

/* Use the tabla leads_merged for create the variable Addition in the table used to the Adds&Retracts report */
data leads_merged_new;
   set leads_merged;
   if STATUS_ADDR = 'AR_NEW';
run;

PROC SQL;
   CREATE TABLE addition AS 
   SELECT t1.ID_CAMPAIGN AS CAMPAIGN_CD,
	  t1.ID_COMMUNICATION AS COMMUNICATION_CD,
          (COUNT(t1.ID_CAMPAIGN)) AS ADDITION
   FROM leads_merged_new t1
   GROUP BY t1.ID_CAMPAIGN, t1.ID_COMMUNICATION;
QUIT;


/* Delete leads_retracted table */
proc delete data= leads_merged_new;
run;

%macro createRetractedFile;
   %if %sysfunc(exist(leads_retracted)) %then %do;
	/* Use the tabla leads_retracted for create the variable Retraction in the table used to the Adds&Retracts report */
		PROC SQL;
		   CREATE TABLE retraction AS
		   SELECT t1.ID_CAMPAIGN AS CAMPAIGN_CD,
        		  t1.ID_COMMUNICATION AS COMMUNICATION_CD,
		          (COUNT(t1.ID_CAMPAIGN)) AS RETRACTION
		   FROM leads_retracted t1
		   GROUP BY t1.ID_CAMPAIGN, t1.ID_COMMUNICATION;
		QUIT;

	/* Generate file RetracFile */
		proc sort data = leads_retracted;
			by id_lead;
		run;
		proc sql noprint;
			select count(*) into: obscnt from leads_retracted where export=1;
		quit;
   %end;
   %else %do;
	%let obscnt=0;
   %end;
	
   %if &obscnt=0 %then %do;
        X "touch &file_retracted.";
   %end;
   %else %do;
  	  /* Put 'SetState' into ACTION variable*/
      data RetracFile;
            retain action id_lead statecode statuscode outcome secundary_outcome;
            format ACTION ID_LEAD;
            set leads_retracted; 
		        where export=1;

            ACTION='U';
            STATECODE=1;
            STATUSCODE=2;
      run;

      filename exp_file "&file_retracted.";
      
	    proc export data=RetracFile(keep=ACTION ID_LEAD STATECODE STATUSCODE OUTCOME SECUNDARY_OUTCOME) dbms=CSV outfile=exp_file replace;
        	delimiter= ';';
        	putnames=no;
     run;

	  /* Delete leads_retracted table */
  	proc delete data= RetracFile;
  	run;

	  /* Save Retracted Leads into history */
  	data leads_retracted;
  		set leads_retracted 
			(drop= HISTORY CHANNEL_CD EXPIRATION_DATE LEAD_KIND EXPORT IND_SGL ASSIGNED_DATE);
  	run;

  	/* Append deleted files to history */
  	proc append base=leads_history data=leads_retracted;
  	run;

  	/* Delete leads_retracted table */
  	proc delete data= leads_retracted;
  	run;

  %end;
%mend createRetractedFile;
%createRetractedFile;

/* Join variables Addition and Retraction to the table used to the Adds&Retracts report */
PROC SQL;
   CREATE TABLE join_addition AS
   SELECT coalesce(t1.CAMPAIGN_CD,t2.CAMPAIGN_CD) AS CAMPAIGN_CD,
	  coalesce(t1.COMMUNICATION_CD,t2.COMMUNICATION_CD) AS COMMUNICATION_CD,
	  t1.CHANNEL,
	  t1.SLR_TODAY,
	  t1.SLR_PREVIOUS,
          t2.ADDITION
      FROM records_leads t1 FULL JOIN addition t2
      ON (upcase(t1.CAMPAIGN_CD) = upcase(t2.CAMPAIGN_CD) AND upcase(t1.COMMUNICATION_CD) = upcase(t2.COMMUNICATION_CD));
QUIT;

/* Delete leads_retracted table */
proc delete data=addition;
run;

PROC SQL;
   CREATE TABLE &scpc_lib..records_leads AS
   SELECT coalesce(t1.CAMPAIGN_CD,t2.CAMPAIGN_CD) AS CAMPAIGN_CD,
	  coalesce(t1.COMMUNICATION_CD,t2.COMMUNICATION_CD) AS COMMUNICATION_CD,
	  t1.CHANNEL,
	  t1.SLR_TODAY,
	  t1.SLR_PREVIOUS,
	  t1.ADDITION,
          t2.RETRACTION
      FROM join_addition t1 FULL JOIN retraction t2
      ON (upcase(t1.CAMPAIGN_CD) = upcase(t2.CAMPAIGN_CD) AND upcase(t1.COMMUNICATION_CD) = upcase(t2.COMMUNICATION_CD));
QUIT;

/* Delete leads_retracted table */
proc delete data= join_addition;
run;

/* Generate DDL CI_LEADS */
%lead_ddl(leadsTableName=CI_LEADS_ORA_TODAY);
%error_batch;

/*This proc uploads the new CI_LEADS to the temporary table*/
proc sql;
        insert into &cdm_tsb_lib..CI_LEADS_ORA_TODAY
        SELECT
                  ID_LEAD, PARTY_ID, ID_CAMPAIGN, ID_COMMUNICATION, ID_CELL, PRIORITY,
                  STATUS, OUTCOME, SECUNDARY_OUTCOME, CREATION_DATE, DATA_TOKEN,
                  ASSIGNED_USER, UPDATE_USER, AREA, NETWORK, SORT_CODE, EXPIRY_DATE, 
		  LEAD_CR, CHANNEL_CR, IND_SGL, ASSIGNED_DATE
        FROM LEADS_MERGED;
quit;

%error_batch;

/*This proc deletes the old CI_LEADS and it renames the temporary table as the new CI_LEADS*/

proc sql;
  connect to oracle (PATH=&orapath. USER=&cdm_user. PASS="&cdm_pass");
  execute(drop table &cdm_schema..CI_LEADS) by oracle;
  execute(alter table &cdm_schema..CI_LEADS_ORA_TODAY
       rename to CI_LEADS) by oracle;
   disconnect from oracle;
quit;


%error_batch;

/* Quitar cuando Luis termine d eprobar */
/*
proc sql;
  connect to oracle (PATH=&orapath. USER=&cdm_user. PASS="&cdm_pass");
  execute(drop table &cdm_schema..CI_LEADS_ORA_TODAY) by oracle;
  disconnect from oracle;
quit;
*/

%load_leads_table;

/* Update subprocesses table */
%exec_subprocess_end(sonProc=ADDS_RETRACTS);

