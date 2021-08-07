stati_date=`date -d -1days '+%Y-%m-%d'`
{
    hive -hivevar day=$stati_date -f ./ods/ods__wms_product_label_merge/wms_product_label_merge.sql
} || {
    echo 'task fail: wms_product_label_merge !!!!!!!'
    python ./common/push_ding.py 'wms_product_label_merge'
}
