#!/bin/bash

echo "##################################################################"
echo "################## PII DELETION PROCESS START ####################"
echo "##################################################################"

today=$(date "+%Y%m%d%H%M%S")

echo "################## right now!! : ${today}"

target_table=( "application_train" "credit_card_balance" "installments_payments" "pos_cash_balance" "previous_application" "bureau")
#target_table=( "credit_card_balance" )

for target_table in ${target_table[@]}; do
    sh /home/hadoop/scripts/pipa_deletion_job.sh ${target_table} > /home/hadoop/logs/${target_table}_pipa_${today}.txt &
    sleep 5
done

wait

echo "##################################################################"
echo "################## PII DELETION PROCESS END ######################"
echo "##################################################################"