stati_date=`date -d -1days '+%Y-%m-%d'`
{
    hive -hivevar day=$stati_date -f ./dwd/dwd__dwd_business_vvic_user_account_daily/dwd_business_vvic_user_account_daily.sql
} || {
    echo 'task fail: dwd_business_vvic_user_account_daily !!!!!!!'
    python ./common/push_ding.py 'dwd_business_vvic_user_account_daily'
}
