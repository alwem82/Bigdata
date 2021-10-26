
!echo "########## Start : bureau - table creation in staging"; 

create table staging.bureau(
    sk_id_curr double,
    sk_id_bureau double,
    credit_active varchar(256),
    credit_currency varchar(256),
    days_credit double,
    credit_day_overdue double,
    days_credit_enddate double,
    days_enddate_fact double,
    amt_credit_max_overdue double,
    cnt_credit_prolong double,
    amt_credit_sum double,
    amt_credit_sum_debt double,
    amt_credit_sum_limit double,
    amt_credit_sum_overdue double,
    credit_type varchar(256),
    days_credit_update double,
    amt_annuity double)
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/staging/home_loan/bureau/';

!echo "########## End   : bureau - table creation in staging"; 


!echo "########## Start : bureau - ACID table creation"; 

create table standardization.bureau(
    sk_id_curr double,
    sk_id_bureau double,
    credit_active varchar(256),
    credit_currency varchar(256),
    days_credit double,
    credit_day_overdue double,
    days_credit_enddate double,
    days_enddate_fact double,
    amt_credit_max_overdue double,
    cnt_credit_prolong double,
    amt_credit_sum double,
    amt_credit_sum_debt double,
    amt_credit_sum_limit double,
    amt_credit_sum_overdue double,
    credit_type varchar(256),
    days_credit_update double,
    amt_annuity double)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'serialization.format' = ',',
        'field.delim' = ','
    )
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION 'hdfs://ip-10-0-0-228.ap-northeast-2.compute.internal:8020/octank_datalake/home_loan/standardization/bureau/'
    TBLPROPERTIES ( 
        'transactional'='true',
        'transactional_properties'='default',
        'transient_lastDdlTime'='1555090610'
    );
!echo "########## End   : bureau - ACID table creation"; 

!echo "########## Start : Insert bureau data from staging to ACID table"; 
Insert into standardization.bureau select * from staging.bureau;
!echo "########## End   : Insert bureau data from staging to ACID table"; 

!echo "########## Start : bureau - reporting table creation"; 

create table reporting.bureau(
    sk_id_curr double,
    sk_id_bureau double,
    credit_active varchar(256),
    credit_currency varchar(256),
    days_credit double,
    credit_day_overdue double,
    days_credit_enddate double,
    days_enddate_fact double,
    amt_credit_max_overdue double,
    cnt_credit_prolong double,
    amt_credit_sum double,
    amt_credit_sum_debt double,
    amt_credit_sum_limit double,
    amt_credit_sum_overdue double,
    credit_type varchar(256),
    days_credit_update double,
    amt_annuity double)
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/reporting/home_loan/bureau/';

!echo "########## End   : bureau - reporting table creation"; 

!echo "########## Start : Insert bureau data from SD to RP table"; 
Insert into reporting.bureau select * from standardization.bureau;
!echo "########## End   : Insert bureau data from SD to RP table"; 