hive -e "drop table if exists ods.vvic_order_details_subject"
hive -f ./ods/ods__vvic_order_details_subject/vvic_order_details.sql
python /root/soft/datax/datax/bin/datax.py ./ods/ods__vvic_order_details_subject/vvic_order_details.json