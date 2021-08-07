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
    	hive -hivevar day=`date -d -1month '+%Y%m'` -f ./dw_rpt_pangge/rpt_pangge_user_remain_monthly/rpt_pangge_user_remain_monthly.sql
    } || {
        echo 'task fail:rpt_pangge_user_remain_monthly !!!!!!'
    	python ./push_ding.py 'rpt_pangge_user_remain_monthly'
    }
fi