
!echo "########## Start : installments_payments - external table creation in staging"; 
Create table staging.installments_payments(
    SK_ID_PREV double,
    SK_ID_CURR double,
    NUM_INSTALMENT_VERSION double,
    NUM_INSTALMENT_NUMBER double,
    DAYS_INSTALMENT double,
    DAYS_ENTRY_PAYMENT double,
    AMT_INSTALMENT double,
    AMT_PAYMENT double)
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/staging/home_loan/installments_payments/';

!echo "########## End   : installments_payments - table creation in staging"; 


!echo "########## Start : installments_payments - ACID table creation in standardization"; 

Create table standardization.installments_payments(
    SK_ID_PREV double,
    SK_ID_CURR double,
    NUM_INSTALMENT_VERSION double,
    NUM_INSTALMENT_NUMBER double,
    DAYS_INSTALMENT double,
    DAYS_ENTRY_PAYMENT double,
    AMT_INSTALMENT double,
    AMT_PAYMENT double)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'serialization.format' = ',',
        'field.delim' = ','
    )
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION 'hdfs://ip-10-0-0-228.ap-northeast-2.compute.internal:8020/octank_datalake/home_loan/standardization/installments_payments/'
    TBLPROPERTIES ( 
        'transactional'='true',
        'transactional_properties'='default',
        'transient_lastDdlTime'='1555090610'
    );
!echo "########## End   : installments_payments - ACID table creation in standardization"; 

!echo "########## Start : Insert installments_payments data from staging to ACID table"; 
Insert into standardization.installments_payments select * from staging.installments_payments;
!echo "########## End   : Insert installments_payments data from staging to ACID table"; 


!echo "########## Start : installments_payments - reporting table creation"; 
Create table reporting.installments_payments(
    SK_ID_PREV double,
    SK_ID_CURR double,
    NUM_INSTALMENT_VERSION double,
    NUM_INSTALMENT_NUMBER double,
    DAYS_INSTALMENT double,
    DAYS_ENTRY_PAYMENT double,
    AMT_INSTALMENT double,
    AMT_PAYMENT double)
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/reporting/home_loan/installments_payments/';

!echo "########## End   : installments_payments - reporting table creation";

!echo "########## Start : Insert installments_payments data from SD to RP table"; 
Insert into reporting.installments_payments select * from standardization.installments_payments;
!echo "########## End   : Insert installments_payments data from SD to RP table"; 
