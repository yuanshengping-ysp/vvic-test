hive -e "drop table if exists ods.vvic_user_trade_subject"
hive -f ./ods/ods__vvic_user_trade_subject/vvic_user_trade.sql 
python /root/soft/datax/datax/bin/datax.py ./ods/ods__vvic_user_trade_subject/vvic_user_trade.json