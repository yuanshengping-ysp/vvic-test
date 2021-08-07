stati_date=`date -d -1days '+%Y-%m-%d'`
{
    hive -hivevar day=$stati_date -f ./ods/ods__vvic_order_merge/vvic_order_merge.sql
} || {
    echo 'task fail: vvic_order_merge !!!!!!!'
    python ./common/push_ding.py 'vvic_order_merge'
}
