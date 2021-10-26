!echo "########## Start : credit_card_balance - table creation in staging database"; 
Create table staging.credit_card_balance(
SK_ID_PREV double,
SK_ID_CURR double,
MONTHS_BALANCE double,
AMT_BALANCE double,
AMT_CREDIT_LIMIT_ACTUAL double,
AMT_DRAWINGS_ATM_CURRENT double,
AMT_DRAWINGS_CURRENT double,
AMT_DRAWINGS_OTHER_CURRENT double,
AMT_DRAWINGS_POS_CURRENT double,
AMT_INST_MIN_REGULARITY double,
AMT_PAYMENT_CURRENT double,
AMT_PAYMENT_TOTAL_CURRENT double,
AMT_RECEIVABLE_PRINCIPAL double,
AMT_RECIVABLE double,
AMT_TOTAL_RECEIVABLE double,
CNT_DRAWINGS_ATM_CURRENT double,
CNT_DRAWINGS_CURRENT double,
CNT_DRAWINGS_OTHER_CURRENT double,
CNT_DRAWINGS_POS_CURRENT double,
CNT_INSTALMENT_MATURE_CUM double,
NAME_CONTRACT_STATUS varchar(256),
SK_DPD double,
SK_DPD_DEF double
)STORED AS ORC
  LOCATION
  's3://octank-datalake-seba/staging/home_loan/credit_card_balance/';

!echo "########## End   : credit_card_balance - table creation in staging database"; 


!echo "########## Start : credit_card_balance - ACID table creation in standardization database"; 
Create table standardization.credit_card_balance(
    SK_ID_PREV double,
    SK_ID_CURR double,
    MONTHS_BALANCE double,
    AMT_BALANCE double,
    AMT_CREDIT_LIMIT_ACTUAL double,
    AMT_DRAWINGS_ATM_CURRENT double,
    AMT_DRAWINGS_CURRENT double,
    AMT_DRAWINGS_OTHER_CURRENT double,
    AMT_DRAWINGS_POS_CURRENT double,
    AMT_INST_MIN_REGULARITY double,
    AMT_PAYMENT_CURRENT double,
    AMT_PAYMENT_TOTAL_CURRENT double,
    AMT_RECEIVABLE_PRINCIPAL double,
    AMT_RECIVABLE double,
    AMT_TOTAL_RECEIVABLE double,
    CNT_DRAWINGS_ATM_CURRENT double,
    CNT_DRAWINGS_CURRENT double,
    CNT_DRAWINGS_OTHER_CURRENT double,
    CNT_DRAWINGS_POS_CURRENT double,
    CNT_INSTALMENT_MATURE_CUM double,
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
    LOCATION 'hdfs://ip-10-0-0-228.ap-northeast-2.compute.internal:8020/octank_datalake/home_loan/standardization/credit_card_balance/'
    TBLPROPERTIES ( 
        'transactional'='true',
        'transactional_properties'='default',
        'transient_lastDdlTime'='1555090610'
    );
!echo "########## End   : credit_card_balance - ACID table creation in standardization database"; 

!echo "########## Start : Insert credit_card_balance data from staing to ACID table"; 
Insert into standardization.credit_card_balance select * from staging.credit_card_balance;
!echo "########## End   : Insert credit_card_balance data from staging to ACID table"; 

!echo "########## Start : credit_card_balance - reporting table creation"; 
Create table reporting.credit_card_balance(
SK_ID_PREV double,
SK_ID_CURR double,
MONTHS_BALANCE double,
AMT_BALANCE double,
AMT_CREDIT_LIMIT_ACTUAL double,
AMT_DRAWINGS_ATM_CURRENT double,
AMT_DRAWINGS_CURRENT double,
AMT_DRAWINGS_OTHER_CURRENT double,
AMT_DRAWINGS_POS_CURRENT double,
AMT_INST_MIN_REGULARITY double,
AMT_PAYMENT_CURRENT double,
AMT_PAYMENT_TOTAL_CURRENT double,
AMT_RECEIVABLE_PRINCIPAL double,
AMT_RECIVABLE double,
AMT_TOTAL_RECEIVABLE double,
CNT_DRAWINGS_ATM_CURRENT double,
CNT_DRAWINGS_CURRENT double,
CNT_DRAWINGS_OTHER_CURRENT double,
CNT_DRAWINGS_POS_CURRENT double,
CNT_INSTALMENT_MATURE_CUM double,
NAME_CONTRACT_STATUS varchar(256),
SK_DPD double,
SK_DPD_DEF double
)STORED AS ORC
  LOCATION
  's3://octank-datalake-seba/reporting/home_loan/credit_card_balance/';

!echo "########## End   : credit_card_balance - staging table creation"; 

!echo "########## Start : Insert credit_card_balance data from SD to RP table"; 
Insert into reporting.credit_card_balance select * from standardization.credit_card_balance;
!echo "########## End   : Insert credit_card_balance data from SD to RP table"; 