echo "##################################################################"
echo "################## PII DELETION PROCESS START ####################"
echo "##################################################################"

today=$(date "+%Y%m%d%H%M%S")

echo "################## right now!! : ${today}"

target_table=( "application_train" "credit_card_balance" "installments_payments" "pos_cash_balance" "previous_application" "bureau")
#target_table=( "credit_card_balance" )

for target_table in ${target_table[@]}; do

    echo "################## Target Table Name : ${target_table}"

    var1=`hive -e "select count(*) from standardization.${target_table}"`
    var2=`hive -e "select count(*) from standardization.${target_table} where sk_id_curr not in (select id from standardization.pii_list)"`

    if [ "${var1}" = "${var2}" ]; then
 	    echo "standardization.${target_table} - No deletion target data existed"
    else   
        #var3=`hive -e "select count(*) from standardization.${target_table} where sk_id_curr not in (select id from staging.pii_list)"`
        hive -e "delete from standardization.${target_table} where sk_id_curr in (select id from standardization.pii_list)"
        var3=`hive -e "select count(*) from standardization.${target_table}"`

        echo "################## standardization.${target_table} totoal record                     : ${var1}"
        echo "################## standardization.${target_table} non-target record before deletion : ${var2}"
        echo "################## standardization.${target_table} totoal record after deletion      : ${var3}"


        if [ "${var2}" = "${var3}" ]; then
            #echo "################## Truncate and COPY date from standardization.${target_table}  to reporting.${target_table} "
            #hive -e "truncate table reporting.${target_table}"
            #hive -e "insert into reporting.${target_table} select * from standardization.${target_table}"
            echo "################## PII deletion count mathced. PII_STATISTICS table update started"
            hive -e "insert into standardization.pii_statistics values ('${target_table}','default','${var1}','${var2}','${var3}','OK','${today}')"
        else
            echo "################## PII deletion count MISMATCHED!!!"
            hive -e "insert into standardization.pii_statistics values ('${target_table}','default','${var1}','${var2}','${var3}','Failed','${today}')"
        fi
    fi
done


echo "##################################################################"
echo "################## PII DELETION PROCESS END ######################"
echo "##################################################################"