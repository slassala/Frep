/*
Crear dos macro funciones

Una que sea incluir_columna que reciba como parámetros la nueva columna con la definicion y 
la tabla en la que hay que insertarla, y esa macro lo unico que tendra sera un proc sql; 
con un alter table 

Luego otra macro función en la que el primer paso sea hacer un proc contents de la tabla 
donde se va insertar el nuevo campo y despues cruzar esa tabla con la tabla de config_fich
por campo y el cruce que sea con un left join de tal manera que me queden aquellos campos que 
estan en la config fich y no estan en el proc contect 

Proc contents de la tabla de de histórico ( tablas del tipo FB, PC ,RE)
left join entre la tabla de config_fich filtrando por proveedor y la salida del proc contents, 
quedándonos con aquellos registros de la config_fich que no están en el proc contents
Por cada registro resultante de la consulta anterior llamar a la primera macro.
*/

libname HIS_COMM '/DATOS/sasdata/His_Comm';
libname CONFICH '/DATOS/sasdata/Param/';
OPTION SPOOL;

%macro add_column(lib, table, column, datatype, label, long);
%let col = %lowcase(&column.);

/*Se evalua si la columna ya existe en proc contents*/
proc contents data=&lib..&table out=test noprint; run;  
proc sql noprint; select count(1) into :cant  from test where lowcase(name) in ("&col.");run; 

/*Si no existe inserta*/
%if &cant eq 0 %then
	%if  %upcase("&datatype") eq "CHAR" %then
	%do;
		/*Si la longitud del char es un nro erroeno o vacio, setea a default*/
	    %if &long <= 0 or %sysfunc(trim(&long)) eq '' %then %let long=8; 
		proc sql;
	    alter table &lib..&table add &col &datatype(&long) label="&label" /*format=&format*/;
	    quit;
	%end; 
	%else
	%do;
		/*Si el tipo de dato es NUMBER da error entonces se cambia a NUMERIC*/
		%if %sysfunc(trim(%sysfunc(left(%upcase("&datatype"))))) eq "NUMBER" %then %let datatype=NUMERIC; 
		proc sql;
	    alter table &lib..&table add &col &datatype label="&label";
	    quit;
	%end; 
%else %put La columna ya existe;

%mend add_column;
 
%add_column(His_Comm, FB_c, campo11, char , campo11,  'sarasa'); 

options symbolgen mcompile ;

%macro add_col_in_hist_from_config(libref_hist, table_hist);
%let tabl= %upcase(&table_hist.);

/*Se genera una tabla con la definicion actual de la tabla a insertar*/
proc contents data=&libref_hist..&tabl out=test_&tabl noprint; run;  

/*Datos a insertar en la tabla historico*/
proc sql noprint; 
create table info_a_insertar_en_prov as
	select 
	trim(left(lowcase(cf.field_name))) as field_name,  
	upcase(cf.tipo) as tipo, 
	cf.data_type,
	cf.length,
	cf.position,
	cf.sort

	from confich.config_fich_cpy cf 

	left join test_&tabl t
	on trim(left(lowcase(cf.field_name))) =  trim(left(lowcase(t.name)))

	where  upcase(cf.tipo) = "&tabl."
    and  trim(left(lowcase(t.name))) is null;
run;

/*Cantidad a insertar*/
proc sql noprint; select count(1) into :nobs from info_a_insertar_en_prov;run; 

/*Itera por cada columna a insertar*/
%do i=1 %to &nobs;
	data _null_;
	set info_a_insertar_en_prov(firstobs=&i obs=&i);
	call symputx('campo_h', %sysfunc(trim(%sysfunc(left(field_name)))));
	call symputx('tabla_h', %sysfunc(trim(%sysfunc(left(tipo)))));
	call symputx('dtype_h', %sysfunc(trim(%sysfunc(left(data_type)))));
	call symputx('longi_h', %sysfunc(trim(%sysfunc(left(length)))));
	run;
		
	%add_column(&libref_hist, &tabl, &campo_h, &dtype_h, &campo_h, &longi_h);
%end;
%mend add_col_in_hist_from_config;

%add_col_in_hist_from_config(His_CoMm, FB_c);

/* 
#
# PRUEBAS
#
*/
proc sql; describe table his_comm.FB_C;run;  
proc sql; select * from  his_comm.FB_c;run;
proc sql; alter table his_comm.FB_c drop id_customer, file,fecha_nueva, fecha_nueva2, campo3, campo4,campo5,campo6, campo7,campo8,campo9,campo10,campo11; run; 
proc sql; select distinct tipo from confich.config_fich_cpy; run; 
proc sql; select * from confich.config_fich_cpy where tipo = 'FB_c';run; 
proc sql; 
insert into confich.config_fich_cpy 
select 
	Field_Name,
	'FB_c',
	Data_Type,
	Length,
	Position,
	sort
from confich.config_fich where tipo = 'FB' and lowcase(field_name) not in ('email_address', 'fecha');run;



