!echo "########## Start : previous_application - table creation in staging database"; 

CREATE TABLE staging.previous_application(
    sk_id_prev double,
    sk_id_curr double,
    name_contract_type varchar(256),
    amt_annuity double,
    amt_application double,
    amt_credit double,
    amt_down_payment double,
    amt_goods_price double,
    weekday_appr_process_start varchar(256),
    hour_appr_process_start double,
    flag_last_appl_per_contract varchar(256),
    nflag_last_appl_in_day double,
    rate_down_payment varchar(256),
    rate_interest_primary double,
    rate_interest_privileged double,
    name_cash_loan_purpose varchar(256),
    name_contract_status varchar(256),
    days_decision double,
    name_payment_type varchar(256),
    code_reject_reason varchar(256),
    name_type_suite varchar(256),
    name_client_type varchar(256),
    name_goods_category varchar(256),
    name_portfolio varchar(256),
    name_product_type varchar(256),
    channel_type varchar(256),
    sellerplace_area double,
    name_seller_industry varchar(256),
    cnt_payment double,
    name_yield_group varchar(256),
    product_combination varchar(256),
    days_first_drawing double,
    days_first_due double,
    days_last_due_1st_version double,
    days_last_due double,
    days_termination double,
    nflag_insured_on_approval double)
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/staging/home_loan/previous_application/';

!echo "########## End   : previous_application - table creation in staging database"; 


!echo "########## Start : previous_application - ACID table creation in standardization database"; 

CREATE TABLE standardization.previous_application(
    sk_id_prev double,
    sk_id_curr double,
    name_contract_type varchar(256),
    amt_annuity double,
    amt_application double,
    amt_credit double,
    amt_down_payment double,
    amt_goods_price double,
    weekday_appr_process_start varchar(256),
    hour_appr_process_start double,
    flag_last_appl_per_contract varchar(256),
    nflag_last_appl_in_day double,
    rate_down_payment varchar(256),
    rate_interest_primary double,
    rate_interest_privileged double,
    name_cash_loan_purpose varchar(256),
    name_contract_status varchar(256),
    days_decision double,
    name_payment_type varchar(256),
    code_reject_reason varchar(256),
    name_type_suite varchar(256),
    name_client_type varchar(256),
    name_goods_category varchar(256),
    name_portfolio varchar(256),
    name_product_type varchar(256),
    channel_type varchar(256),
    sellerplace_area double,
    name_seller_industry varchar(256),
    cnt_payment double,
    name_yield_group varchar(256),
    product_combination varchar(256),
    days_first_drawing double,
    days_first_due double,
    days_last_due_1st_version double,
    days_last_due double,
    days_termination double,
    nflag_insured_on_approval double)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'serialization.format' = ',',
        'field.delim' = ','
    )
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION 'hdfs://ip-10-0-0-228.ap-northeast-2.compute.internal:8020/octank_datalake/home_loan/standardization/previous_application/'
    TBLPROPERTIES ( 
        'transactional'='true',
        'transactional_properties'='default',
        'transient_lastDdlTime'='1555090610'
    );
!echo "########## End   : previous_application - ACID table creation in standardization database"; 

!echo "########## Start : Insert previous application data from staging to ACID table"; 
Insert into standardization.previous_application select * from staging.previous_application;
!echo "########## End   : Insert previous_application data from staging to ACID table"; 

!echo "########## Start : previous_application - reporting table creation"; 
CREATE TABLE reporting.previous_application(
    sk_id_prev double,
    sk_id_curr double,
    name_contract_type varchar(256),
    amt_annuity double,
    amt_application double,
    amt_credit double,
    amt_down_payment double,
    amt_goods_price double,
    weekday_appr_process_start varchar(256),
    hour_appr_process_start double,
    flag_last_appl_per_contract varchar(256),
    nflag_last_appl_in_day double,
    rate_down_payment varchar(256),
    rate_interest_primary double,
    rate_interest_privileged double,
    name_cash_loan_purpose varchar(256),
    name_contract_status varchar(256),
    days_decision double,
    name_payment_type varchar(256),
    code_reject_reason varchar(256),
    name_type_suite varchar(256),
    name_client_type varchar(256),
    name_goods_category varchar(256),
    name_portfolio varchar(256),
    name_product_type varchar(256),
    channel_type varchar(256),
    sellerplace_area double,
    name_seller_industry varchar(256),
    cnt_payment double,
    name_yield_group varchar(256),
    product_combination varchar(256),
    days_first_drawing double,
    days_first_due double,
    days_last_due_1st_version double,
    days_last_due double,
    days_termination double,
    nflag_insured_on_approval double)
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/reporting/home_loan/previous_application/';

!echo "########## End   : previous_application - reporting table creation"; 

!echo "########## Start : Insert previous application data from SD to RP table"; 
Insert into reporting.previous_application select * from standardization.previous_application;
!echo "########## End   : Insert previous_application data from SD to RP table"; 