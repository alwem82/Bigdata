
!echo "########## Start : pos_cash_balance - table creation in staging"; 
Create table staging.pos_cash_balance(
    SK_ID_PREV double,
    SK_ID_CURR double,
    MONTHS_BALANCE double,
    CNT_INSTALMENT double,
    CNT_INSTALMENT_FUTURE double,
    NAME_CONTRACT_STATUS varchar(256),
    SK_DPD double,
    SK_DPD_DEF double)
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/staging/home_loan/pos_cash_balance/';
!echo "########## End   : pos_cash_balance - table creation in staging"; 

!echo "########## Start : pos_cash_balance - ACID table creation in standardization"; 

Create table standardization.pos_cash_balance(
    SK_ID_PREV double,
    SK_ID_CURR double,
    MONTHS_BALANCE double,
    CNT_INSTALMENT double,
    CNT_INSTALMENT_FUTURE double,
    NAME_CONTRACT_STATUS varchar(256),
    SK_DPD double,
    SK_DPD_DEF double)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'serialization.format' = ',',
        'field.delim' = ','
    )
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION 'hdfs://ip-10-0-0-228.ap-northeast-2.compute.internal:8020/octank_datalake/home_loan/standardization/pos_cash_balance/'
    TBLPROPERTIES ( 
        'transactional'='true',
        'transactional_properties'='default',
        'transient_lastDdlTime'='1555090610'
    );
!echo "########## End   : pos_cash_balance - ACID table creation in standardization"; 

!echo "########## Start : Insert pos_cash_balance data from staging to ACID table"; 
Insert into standardization.pos_cash_balance select * from staging.pos_cash_balance;
!echo "########## End   : Insert pos_cash_balance data from staging to ACID table"; 


!echo "########## Start : pos_cash_balance - reporting table creation"; 
Create table reporting.pos_cash_balance(
    SK_ID_PREV double,
    SK_ID_CURR double,
    MONTHS_BALANCE double,
    CNT_INSTALMENT double,
    CNT_INSTALMENT_FUTURE double,
    NAME_CONTRACT_STATUS varchar(256),
    SK_DPD double,
    SK_DPD_DEF double)
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/reporting/home_loan/pos_cash_balance/';
!echo "########## End   : pos_cash_balance - reporting table creation"; 

!echo "########## Start : Insert pos_cash_balance data from SD to RP table"; 
Insert into reporting.pos_cash_balance select * from standardization.pos_cash_balance;
!echo "########## End   : Insert pos_cash_balance data from SD to RP table"; 