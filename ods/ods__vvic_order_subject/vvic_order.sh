hive -e "drop table if exists ods.vvic_order_subject"
hive -f ./ods/ods__vvic_order_subject/vvic_order.sql
python /root/soft/datax/datax/bin/datax.py ./ods/ods__vvic_order_subject/vvic_order.json