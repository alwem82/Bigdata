!echo "########## Start : pii_statistics - ACID table creation in standardization"; 
Create table standardization.pii_statistics(
    table_name varchar(50),
    partition_name varchar(20),
    total_row bigint,
    not_target_row bigint,
    remained_row bigint,
    status varchar(10),
    tran_date varchar(20)
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'serialization.format' = ',',
        'field.delim' = ','
    )
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION 'hdfs://ip-10-0-0-228.ap-northeast-2.compute.internal:8020/octank_datalake/home_loan/standardization/pii_statistics/'
    TBLPROPERTIES ( 
    'transactional'='true',
    'transactional_properties'='default',
    'transient_lastDdlTime'='1555090610'
    );
!echo "########## End : pii_statistics - ACID table creation in standardization"; 

!echo "########## Start : pii_statistics - reporting table creation"; 
Create table reporting.pii_statistics(
    table_name varchar(50),
    partition_name varchar(20),
    total_row bigint,
    not_target_row bigint,
    remained_row bigint,
    status varchar(10),
    tran_date varchar(20))
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/reporting/home_loan/pii_statistics/';

!echo "########## End   : pii_statistics - reporting table creation";

!echo "########## Start : Insert installments_payments data from SD to RP table"; 
Insert into reporting.pii_statistics select * from standardization.pii_statistics;
!echo "########## End   : Insert installments_payments data from SD to RP table"; 