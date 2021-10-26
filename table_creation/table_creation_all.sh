#!/bin/bash

target_table=( "application_train" "credit_card_balance" "installments_payments" "pos_cash_balance" "previous_application" "bureau" "bureau_balance" "pii_list" "pii_statistics")
#target_table=( "credit_card_balance" )

echo "############## All Table Creation Started ##############"

today=$(date "+%Y%m%d%H%M%S")

for target_table in ${target_table[@]}; do
    hive -f /home/hadoop/scripts/${target_table}_creation.hql > /home/hadoop/logs/${target_table}_creation_${today}.txt &
    sleep 5
done

wait

echo "############## All Table Creation Completed ##############"
