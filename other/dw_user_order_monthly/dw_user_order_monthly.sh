# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
   part_day=`date -d -1days '+%Y-%m-01'`
fi

day=`date +%d`

if [ "$day" = 01 ]; then
    {
    	hive -hivevar day=$today -f   ./other/dw_user_order_monthly/dw_user_order_monthly.sql 	
    } || {
        echo 'task fail:dw_user_order_monthly !!!!!!'
    	python ./push_ding.py 'dw_user_order_monthly'
    }
fi