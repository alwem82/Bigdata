

Create external table hstaging.previous_application(
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
    nflag_insured_on_approval double
)ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://octank-datalake-seba/hstaging/home_loan/previous_application/'


Create external table hstandardization.previous_application(
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
    nflag_insured_on_approval double
)ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://octank-datalake-seba/hstandardization/home_loan/previous_application/default/';

