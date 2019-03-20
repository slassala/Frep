/* Macro that generate leads tables with SLR */
%macro generate_leads_table;

    /*Get range of time in order to get SLR Tables */
    proc sql noprint;
	select catx("|",date_start,date_end) INTO: date_exec_camps
	FROM &scpc_lib..SUBPROCESS_CONTROL_&day_exec_yyyymmdd.
	WHERE processId = "&procName."
	AND applicationId = "&applId."
	AND sonprocess = "&dp_exec_campaigns.";
    quit;

    /* Initialize dates to test them */
    %if ^%symexist(date_exec_camps) or %sysfunc(compress("&date_exec_camps.")) = "." or %sysfunc(compress("&date_exec_camps.")) = "" %then %do;
	%let date_exec_camps = %sysfunc(catx(|,%sysevalf(%sysfunc(datetime())-7200), %sysfunc(datetime())));
    %end;

    %let date_init_camps = %qscan(&date_exec_camps,1,%str("|"));
    %let date_end_camps = %qscan(&date_exec_camps,2,%str("|"));

   /*This proc generates a dataset with the name of all de datasets that the process has to read*/

    proc sql;
        create table SLR_tables as
                select memname from dictionary.tables
                where libname=UPCASE("&slr_ar_lib.") 
				and memname like 'SLR%'
				and modate between &date_init_camps. AND &date_end_camps.
                        ORDER BY memname;
     quit;

   
    /*This proc generates a dataset with the name of all de datasets that the process has to read*/
/*     proc sql;
    	create table SLR_tables as
        	select memname from dictionary.tables
         	where libname=UPCASE("&slr_ar_lib.") and memname like 'SLR%'  
			ORDER BY memname;
     quit;
*/

    /* Generate table New Leads*/
     data LEADS_TODAY ;
     	format 
	PARTY_ID 8.
       	ID_CAMPAIGN $char15.
     	ID_COMMUNICATION $char15.
     	ID_CELL $char15.
     	STATUS_CD 8.
     	OUTCOME 8.
     	CREATION_DATE date9.
     	PRIORITY 3.
     	SECUNDARY_OUTCOME 8.
     	ASSIGNED_USER $char255.
     	UPDATE_USER $char10.
     	AREA $char6.
     	NETWORK $char6.
     	SORT_CODE $char6.
	EXPIRY_DATE date9.
	LEAD_CR $char100.
        CHANNEL_CR $char3.
	IND_SGL 8.
	ASSIGNED_DATE date9.

     	length DATA_TOKEN $2000.;
	stop;
     run;

     %let tabla = SLR_tables;
     %let dsid1=%sysfunc(open(&tabla));
     %let limit=%sysfunc(attrn(&dsid1,NOBS));

     data &scpc_lib..leads_tables;
                length LEADTABLE $256 CAMPAIGN_CD $30 COMMUNICATION_CD $30;
		stop;
     run;

     /* If there is no data, bucle does not start */
     %if "&limit." = "" %then %let limit = 0;
 
     /*Start bucle */
     %do i=1 %to &limit.;
	%let rc=%sysfunc(fetchobs(&dsid1,&i));
        %let pos0=%sysfunc(varnum(&dsid1,memname));
        %let table=%sysfunc(getvarc(&dsid1,&pos0));

	data _null_;
		set &slr_ar_lib..&table.;
		if _N_=1 then do;
	                call symput('camp_cd',compress(CAMPAIGN_CD));
	                call symput('comm_cd',compress(COMMUNICATION_CD));
	                stop;
	        end;
	run;

	%put campaign &camp_cd and communication &comm_cd;

	data tablaatabla;
		length LEADTABLE $256 CAMPAIGN_CD $30 COMMUNICATION_CD $30;
		LEADTABLE="&slr_ar_lib..&table.";
		CAMPAIGN_CD="&camp_cd.";
		COMMUNICATION_CD="&comm_cd.";
	run;

	data &scpc_lib..leads_tables;
		set &scpc_lib..leads_tables tablaatabla;
	run;

        /*Beginning code of data token*/

        proc contents data=&slr_ar_lib..&table. out=field_form noprint;
        run;
        proc sort data=field_form;
        	by VARNUM;
        run;

        data _Null_;
        	retain i;
        	set field_form (keep= name format formatl formatd)end= final;
		if _N_=1 then i=0;
          	if (index(upcase('ID_CUSTOMER,CAMPAIGN_CD,COMMUNICATION_CD,CELL_CD,ID_AREA,ID_NETWORK,ID_SORT_CODE'),compress(upcase(name))) eq 0) then do;
            	i=sum(i,1);
                call symput(compress('name'||i),name);
                if format ne '' then
              		call symput(compress('format'||i),compress(format||put(formatl,16.)||'.'||put(formatd,16.)));
            end;
            if final then call symput('Num_camp',i);
        run;
        proc sql;
        	create table comm_channel as
            	select l.*, r.channel_cd
                from &slr_ar_lib..&table. as l left join
                  &cdm_tsb_lib..ci_reference_data  as r
                on l.campaign_cd=r.campaign_cd and l.communication_cd=r.communication_cd;
        quit;

        data comm_channel;
        	set comm_channel;
                length DATA_TOKEN $2000.;

            	/* Create Data token */
                DATA_TOKEN=''
                	%do j=1 %to &Num_camp;

                       		%if &j.=&Num_camp or %sysfunc(mod(&j.,2))=1 %then %do;
                              		%if %Symexist(format&j)=1 %then %do;
							||strip(put(&&&name&j,&&&format&j))||''
					%end;
					%else %do;
                               			||strip(&&&name&j)||''
                        		%end;
                        	%end;
                    		%else %do;
                        		%if %Symexist(format&j)=1 %then %do;
						||strip(put(&&&name&j,&&&format&j))||'|'
	                  		%end;
                        		%else %do;
                                		||strip(&&&name&j)||'|'
                        		%end;
                    		%end;
				/* Drop variable if exist */
				%if %Symexist(format&j)=1 %then %do;
					%Symdel format&j;
				%end;
                    	%end;
                	;

         		/* If web channel, remove whitespaces from token */
                	if CHANNEL_CD = "PWB" then do;
                	        DATA_TOKEN = compress(DATA_TOKEN);
                	end;
			/* Check nulls */ 
			if DATA_TOKEN='.' then do;
                        	DATA_TOKEN='';
            		end;    
         	run;

         /*Ending code of data token*/
         
         %error_batch;


         /*This data step applies the correct formats to all the columns in the dataset and appends tables to all_leads */
         data LEADTEMP&i. (drop=ID_CUSTOMER CAMPAIGN_CD COMMUNICATION_CD CELL_CD ID_AREA ID_NETWORK ID_SORT_CODE
         	rename=(PARTY_ID1=PARTY_ID ID_CAMPAIGN1=ID_CAMPAIGN ID_COMMUNICATION1=ID_COMMUNICATION
          	ID_CELL1=ID_CELL PRIORITY1=PRIORITY DATA_TOKEN1=DATA_TOKEN AREA1=AREA NETWORK1=NETWORK SORT_CODE1=SORT_CODE 
		EXPIRY_DATE1=EXPIRY_DATE ));
            set comm_channel (keep=ID_CUSTOMER CAMPAIGN_CD COMMUNICATION_CD CELL_CD ID_AREA ID_NETWORK ID_SORT_CODE DATA_TOKEN);

	    PARTY_ID1 = ID_CUSTOMER;
	    ID_CAMPAIGN1 = compress(put(CAMPAIGN_CD,$15.));
            ID_COMMUNICATION1 = compress(put(COMMUNICATION_CD,$15.));
            ID_CELL1 = compress(put(CELL_CD,$15.));
            STATUS_CD=110;
            OUTCOME=.;
            *CREATION_DATE=&day_exec.;
            PRIORITY1=0;
            SECUNDARY_OUTCOME=.;
            AREA1=put(ID_AREA,$6.);
            NETWORK1=put(ID_NETWORK,$6.);
            SORT_CODE1=put(ID_SORT_CODE,$6.);
	    EXPIRY_DATE1=input(EXPIRY_DATE, date9.);
	    DATA_TOKEN1 = DATA_TOKEN;
	    LEAD_CR="";
	    CHANNEL_CR="";
	    IND_SGL=0;

            format 
	    PARTY_ID1 8.
            ID_CAMPAIGN1 $char15.
            ID_COMMUNICATION1 $char15.
            ID_CELL1 $char15.
            STATUS_CD 8.
            OUTCOME 8.
            CREATION_DATE date9.
            PRIORITY1 3.
            SECUNDARY_OUTCOME 8.
            AREA1 $char6.
            NETWORK1 $char6.
            SORT_CODE1 $char6.
	    EXPIRY_DATE1 date9.
	    DATA_TOKEN1 $char2000.
	    LEAD_CR $char100.
            CHANNEL_CR $char3.
	    IND_SGL 8.
	    ;
	    
            ASSIGNED_USER='';
            UPDATE_USER='';
         run;

        %error_batch;

        /* Set number of records into a macro variable */
        %empty_table(&slr_ar_lib..&table.);

        %put La macro variable vacio toma el valor &vacio;

        %if &vacio=0 %then %do;
        	data intermedia;
        		set &slr_ar_lib..&table. end=eof;
                	if eof then call symput('numObs',_N_);
        	run;
        %end;
        %if &vacio=1 %then %do;
                %let numObs=0;
        %end;

        data fichero;
                length fichero $256 registros 8;
                fichero="&slr_ar_lib..&table.";
                registros=&numObs.;
	run;

        data &scpc_lib..aux_records_leads;
                set &scpc_lib..aux_records_leads fichero;
        run;

        %put "La tabla tiene &numObs registros";
	/* End set number of variable into a macro variable */

        /*This proc deletes the read dataset*/
	/* Tables generated by SLR are not deleted here */
	/*
	proc delete data=&slr_ar_lib..&table.;
	run;
	%error_batch;
	*/

	/* Sort leads_today to merge it*/
	proc sql;
		create table aux_LEADTEMP&i as
		select
		'LEADTEMP&i' as a,*
		from LEADTEMP&i.
		order by ID_COMMUNICATION, ID_CAMPAIGN, PARTY_ID, ID_CELL, AREA, NETWORK, SORT_CODE;
	quit;

	proc delete data=LEADTEMP&i.;
	run;
	%error_batch;

	/*This proc appends the read dataset with the formats applied to the dataset with all the leads*/
	proc append base=LEADS_TODAY data=aux_LEADTEMP&i. FORCE;
	run;
	%error_batch;
	proc delete data=aux_LEADTEMP&i.;
	run;
	%error_batch;


	%end;/* End of bucle i=1 to limit*/
	%let rc=%sysfunc(close(&dsid1));

	/* Delete token temp table */
	proc delete data=token;
	run;
	%error_batch;

	/* Delete SLR temp table*/
	proc delete data=SLR_TABLES;
	run;
	%error_batch;

	/* Save max id lead into a variable. It will be used to insert ID into new leads. */
 	%get_max_lead(varMaxLead=maxLead);

	/* Get Priority */
	data ci_reference_data_priority (keep=ID_COMMUNICATION ID_CAMPAIGN Priority_);
		set &cdm_tsb_lib..CI_REFERENCE_DATA;
		length ID_COMMUNICATION ID_CAMPAIGN $15.;
		Priority_=coalesce(DEF_PRIORITY_COM, DEFAULT_PRIORITY);
		ID_COMMUNICATION=COMMUNICATION_CD;
		ID_CAMPAIGN=CAMPAIGN_CD;
	run;

	proc sort data=ci_reference_data_priority;
		by id_communication id_campaign;
	run;


	/* Update Priority */ 
	data LEADS_TODAY (drop=priority_);
		merge LEADS_TODAY (in=in_leads) CI_REFERENCE_DATA_PRIORITY (in=in_ci);
		by ID_COMMUNICATION ID_CAMPAIGN ;

		format priority 8.;
		priority=priority_;
		if in_leads;
	run;

%mend generate_leads_table;
