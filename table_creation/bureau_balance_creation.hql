!echo "Start : bureau_balance - table creation in staging database"; 
Create table staging.bureau_balance(
    SK_ID_BUREAU double,
    MONTHS_BALANCE double,
    STATUS varchar(256))
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/staging/home_loan/bureau_balance/';

!echo "########## End   : bureau_balance - table creation in staging database"; 

!echo "########## Start : bureau_balance - ACID table creation in standardization database"; 
Create table standardization.bureau_balance(
    SK_ID_BUREAU double,
    MONTHS_BALANCE double,
    STATUS varchar(256))
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'serialization.format' = ',',
        'field.delim' = ','
    )
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION 'hdfs://ip-10-0-0-228.ap-northeast-2.compute.internal:8020/octank_datalake/home_loan/standardization/bureau_balance/'
    TBLPROPERTIES ( 
        'transactional'='true',
        'transactional_properties'='default',
        'transient_lastDdlTime'='1555090610'
    );
!echo "########## End   : bureau_balance - ACID table creation in standardization database"; 

!echo "########## Start : Insert bureau_balance data from staging to ACID table"; 
Insert into standardization.bureau_balance select * from staging.bureau_balance;
!echo "########## End   : Insert bureau_balance data from external to ACID table";

!echo "########## Start : bureau_balance - reporting table creation"; 
Create table reporting.bureau_balance(
    SK_ID_BUREAU double,
    MONTHS_BALANCE double,
    STATUS varchar(256))
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/reporting/home_loan/bureau_balance/';

!echo "########## End   : bureau_balance - reporting table creation"; 

!echo "########## Start : Insert bureau_balance data from SD to RP table"; 
Insert into reporting.bureau_balance select * from standardization.bureau_balance;
!echo "########## End   : Insert bureau_balance data from SD to RP table";