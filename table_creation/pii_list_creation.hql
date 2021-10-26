!echo "########## Start : pii_list - table creation in staging"; 
Create table staging.pii_list(
    ID double)
STORED AS ORC
LOCATION
's3://octank-datalake-seba/staging/home_loan/pii_list/';
!echo "########## End   : pii_list - table creation in staging"; 


!echo "Start : pii_list - ACID table creation"; 
Create table standardization.pii_list(
    ID double)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'serialization.format' = ',',
        'field.delim' = ','
    )
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION 'hdfs://ip-10-0-0-228.ap-northeast-2.compute.internal:8020/octank_datalake/home_loan/standardization/pii_list/'
    TBLPROPERTIES ( 
        'transactional'='true',
        'transactional_properties'='default',
        'transient_lastDdlTime'='1555090610'
    );
!echo "########## End : pii_list - ACID table creation"; 

!echo "Start : Insert pii_list data from staging to ACID table"; 
Insert into standardization.pii_list select * from staging.pii_list;
!echo "########## End   : Insert pii_list data from staging to ACID table"; 

!echo "########## Start : pii_list - reporting table creation"; 
Create table reporting.pii_list(
    ID double)
STORED AS ORC
LOCATION
's3://octank-datalake-seba/reporting/home_loan/pii_list/';
!echo "########## End   : pii_list - reporting table creation"; 

!echo "########## Start : Insert pii_list data from SD to RP table"; 
Insert into reporting.pii_list select * from standardization.pii_list;
!echo "########## End   : Insert pii_list data from SD to RP table"; 