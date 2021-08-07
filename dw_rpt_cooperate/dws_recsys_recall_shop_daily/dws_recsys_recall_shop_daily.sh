# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./dw_rpt_cooperate/dws_recsys_recall_shop_daily/dws_recsys_recall_shop_daily.sql  
} || {
    echo 'task fail:dws_recsys_recall_shop_daily !!!!!!'
	python ./common/push_ding.py  'dws_recsys_recall_shop_daily'
}