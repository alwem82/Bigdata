
!echo "########## Start : Hive Paramenter setting for ACID table"; 
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
!echo "########## End   : Hive Paramenter setting for ACID table"; 

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



!echo "########## Start : application_train - reporting table creation"; 
Create table reporting.application_train(
    sk_id_curr double,
    target double,
    name_contract_type varchar(256),
    code_gender varchar(256),
    flag_own_car varchar(256),
    flag_own_realty varchar(256),
    cnt_children double,
    amt_income_total double,
    amt_credit double,
    amt_annuity double,
    amt_goods_price double,
    name_type_suite varchar(256),
    name_income_type varchar(256),
    name_education_type varchar(256),
    name_family_status varchar(256),
    name_housing_type varchar(256),
    region_population_relative double,
    days_birth double,
    days_employed double,
    days_registration double,
    days_id_publish double,
    own_car_age double,
    flag_mobil double,
    flag_emp_phone double,
    flag_work_phone double,
    flag_cont_mobile double,
    flag_phone double,
    flag_email double,
    occupation_type varchar(256),
    cnt_fam_members double,
    region_rating_client double,
    region_rating_client_w_city double,
    weekday_appr_process_start varchar(256),
    hour_appr_process_start double,
    reg_region_not_live_region double,
    reg_region_not_work_region double,
    live_region_not_work_region double,
    reg_city_not_live_city double,
    reg_city_not_work_city double,
    live_city_not_work_city double,
    organization_type varchar(256),
    ext_source_1 double,
    ext_source_2 double,
    ext_source_3 double,
    apartments_avg double,
    basementarea_avg double,
    years_beginexpluatation_avg double,
    years_build_avg double,
    commonarea_avg double,
    elevators_avg double,
    entrances_avg double,
    floorsmax_avg double,
    floorsmin_avg double,
    landarea_avg double,
    livingapartments_avg double,
    livingarea_avg double,
    nonlivingapartments_avg double,
    nonlivingarea_avg double,
    apartments_mode double,
    basementarea_mode double,
    years_beginexpluatation_mode double,
    years_build_mode double,
    commonarea_mode double,
    elevators_mode double,
    entrances_mode double,
    floorsmax_mode double,
    floorsmin_mode double,
    landarea_mode double,
    livingapartments_mode double,
    livingarea_mode double,
    nonlivingapartments_mode double,
    nonlivingarea_mode double,
    apartments_medi double,
    basementarea_medi double,
    years_beginexpluatation_medi double,
    years_build_medi double,
    commonarea_medi double,
    elevators_medi double,
    entrances_medi double,
    floorsmax_medi double,
    floorsmin_medi double,
    landarea_medi double,
    livingapartments_medi double,
    livingarea_medi double,
    nonlivingapartments_medi double,
    nonlivingarea_medi double,
    fondkapremont_mode varchar(256),
    housetype_mode varchar(256),
    totalarea_mode double,
    wallsmaterial_mode varchar(256),
    emergencystate_mode varchar(256),
    obs_30_cnt_social_circle double,
    def_30_cnt_social_circle double,
    obs_60_cnt_social_circle double,
    def_60_cnt_social_circle double,
    days_last_phone_change double,
    flag_document_2 double,
    flag_document_3 double,
    flag_document_4 double,
    flag_document_5 Double,
    flag_document_6 Double,
    flag_document_7 Double,
    flag_document_8 Double,
    flag_document_9 Double,
    flag_document_10 Double,
    flag_document_11 Double,
    flag_document_12 Double,
    flag_document_13 Double,
    flag_document_14 Double,
    flag_document_15 Double,
    flag_document_16 Double,
    flag_document_17 Double,
    flag_document_18 Double,
    flag_document_19 Double,
    flag_document_20 Double,
    flag_document_21 Double,
    amt_req_credit_bureau_hour Double,
    amt_req_credit_bureau_day Double,
    amt_req_credit_bureau_week Double,
    amt_req_credit_bureau_mon Double,
    amt_req_credit_bureau_qrt Double,
    amt_req_credit_bureau_year Double)  
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/reporting/home_loan/application_train/';
!echo "########## End   : application_train - reporting table creation"; 

!echo "########## Start : Insert application_train data from SD to RP table"; 
Insert into reporting.application_train select * from standardization.application_train;
!echo "########## End   : Insert application_train data from SD to RP table"; 



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


!echo "########## Start : pii_statistics - reporting table creation"; 
Create table reporting.pii_statistics(
    table_name varchar(50),
    partition_name varchar(20),
    total_row double,
    not_target_row double,
    remained_row double,
    status varchar(10),
    tran_date varchar(20))
    STORED AS ORC
    LOCATION
    's3://octank-datalake-seba/reporting/home_loan/pii_statistics/';

!echo "########## End   : pii_statistics - reporting table creation";

!echo "########## Start : Insert installments_payments data from SD to RP table"; 
Insert into reporting.pii_statistics select * from standardization.pii_statistics;
!echo "########## End   : Insert installments_payments data from SD to RP table"; 