hive -e "drop table if exists ods.wms_product_label_subject"
hive -f ./ods/ods__wms_product_label_subject/wms_product_label.sql
python /root/soft/datax/datax/bin/datax.py ./ods/ods__wms_product_label_subject/wms_product_label.json