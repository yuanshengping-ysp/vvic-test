today=`date -d -1days '+%Y-%m-%d'`
hive -e "drop table if exists ods.fms__vvic_user_account_daily"
hive -f ./ods/ods__fms__vvic_user_account_daily/vvic_user_account_daily.sql 
python /root/soft/datax/datax/bin/datax.py ./ods/ods__fms__vvic_user_account_daily/vvic_user_account_daily.json -p -DDATE=$today