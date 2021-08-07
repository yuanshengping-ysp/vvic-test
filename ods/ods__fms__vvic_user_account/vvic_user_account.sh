today=`date -d -1days '+%Y-%m-%d'`
hive -e "drop table if exists ods.fms__vvic_user_account"
hive -f ./ods/ods__fms__vvic_user_account/vvic_user_account.sql 
python /root/soft/datax/datax/bin/datax.py ./ods/ods__fms__vvic_user_account/vvic_user_account.json -p -DDATE=$today