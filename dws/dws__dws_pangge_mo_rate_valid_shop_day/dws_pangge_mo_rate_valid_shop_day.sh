stati_date=`date -d -1days '+%Y-%m-%d'`
{
    hive -hivevar day=$stati_date -f ./dws/dws__dws_pangge_mo_rate_valid_shop_day/dws_pangge_mo_rate_valid_shop_day.sql
} || {
    echo 'task fail: dws_pangge_mo_rate_valid_shop_day !!!!!!!'
    python ./common/push_ding.py 'dws_pangge_mo_rate_valid_shop_day'
}
