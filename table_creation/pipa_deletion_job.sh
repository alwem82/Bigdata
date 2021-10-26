#!/bin/bash

today=$(date "+%Y%m%d%H%M%S")

echo "################## Target Table Name : $1 - Triggered"

var1=`hive -e "select count(*) from standardization.$1"`
var2=`hive -e "select count(*) from standardization.$1 where sk_id_curr not in (select id from standardization.pii_list)"`

if [ "${var1}" = "${var2}" ]; then
    echo "standardization.$1 - No deletion target data existed"
else   
    #var3=`hive -e "select count(*) from standardization.$1 where sk_id_curr not in (select id from staging.pii_list)"`
    hive -e "delete from standardization.$1 where sk_id_curr in (select id from standardization.pii_list)"
    var3=`hive -e "select count(*) from standardization.$1"`

    echo "################## standardization.$1 totoal record                     : ${var1}"
    echo "################## standardization.$1 non-target record before deletion : ${var2}"
    echo "################## standardization.$1 totoal record after deletion      : ${var3}"


    if [ "${var2}" = "${var3}" ]; then
        #echo "################## Truncate and COPY date from standardization.$1  to reporting.$1 "
        #hive -e "truncate table reporting.$1"
        #hive -e "insert into reporting.$1 select * from standardization.$1"
        echo "################## PII deletion count mathced. PII_STATISTICS table update started"
        hive -e "insert into standardization.pii_statistics values ('$1','default','${var1}','${var2}','${var3}','OK','${today}')"
    else
        echo "################## PII deletion count MISMATCHED!!!"
        hive -e "insert into standardization.pii_statistics values ('$1','default','${var1}','${var2}','${var3}','Failed','${today}')"
    fi
fi


echo "################## Target Table Name : $1 - Finished"