echo "##################################################################"
echo "################## PII DELETION PROCESS START ####################"
echo "##################################################################"

today=$(date "+%Y%m%d%H%M%S")

echo "################## right now!! : ${today}"

target_table=( "application_train" "credit_card_balance" "installments_payments" "pos_cash_balance" "previous_application" "bureau")
#target_table=( "credit_card_balance" )

for target_table in ${target_table[@]}; do

    echo "################## Target Table Name : ${target_table}"

    var=`hive -e "select count(*) from standardization.${target_table} where sk_id_curr in (select id from standardization.pii_list)"`

    if [ "${var}" = 0 ]; then
 	    echo "################## standardization.${target_table} - No deletion target data existed"
    else   
        echo "################## standardization.${target_table} - Deletion in progress"
        hive -e "delete from standardization.${target_table} where sk_id_curr in (select id from standardization.pii_list)"
        echo "################## standardization.${target_table} - Deletion completed"
    fi
done


echo "##################################################################"
echo "################## PII DELETION PROCESS END ######################"
echo "##################################################################"

