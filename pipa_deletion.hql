!echo "########## Start : Hive Paramenter setting for ACID table"; 
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
!echo "########## End   : Hive Paramenter setting for ACID table"; 





!echo "########## Start : previous_application - PIPA deletion"; 

select count(*) from standardization.previous_application where sk_id_curr in (
    select id from staging.pii_list
);


delete from standardization.previous_application where sk_id_curr in (
    select id from staging.pii_list
);

!echo "########## End : previous_application - PIPA deletion"; 






!echo "########## Start : application_train_- PIPA deletion"; 

select count(*) from standardization.application_train where sk_id_curr in (
    select id from staging.pii_list
);

delete from standardization.application_train where sk_id_curr in (
    select id from staging.pii_list
);

!echo "########## End : application_train - PIPA deletion"; 





!echo "########## Start : credit_card_balance - PIPA deletion"; 

select count(*) from standardization.credit_card_balance where sk_id_curr in (
    select id from staging.pii_list
);

delete from standardization.credit_card_balance where sk_id_curr in (
    select id from staging.pii_list
);

!echo "########## End : credit_card_balance - PIPA deletion"; 





!echo "########## Start : bureau - PIPA deletion"; 

select count(*) from standardization.bureau where sk_id_curr in (
    select id from staging.pii_list
);

delete from standardization.bureau where sk_id_curr in (
    select id from staging.pii_list
);

!echo "########## End : bureau - PIPA deletion"; 




!echo "########## Start : pos_cash_balance - PIPA deletion"; 

select count(*) from standardization.pos_cash_balance where sk_id_curr in (
    select id from staging.pii_list
);

delete from standardization.pos_cash_balance where sk_id_curr in (
    select id from staging.pii_list
);

!echo "########## End : pos_cash_balance - PIPA deletion"; 




!echo "########## Start : installments_payments - PIPA deletion"; 

select count(*) from standardization.installments_payments where sk_id_curr in (
    select id from staging.pii_list
);

delete from standardization.installments_payments where sk_id_curr in (
    select id from staging.pii_list
);

!echo "########## End : installments_payments - PIPA deletion";
