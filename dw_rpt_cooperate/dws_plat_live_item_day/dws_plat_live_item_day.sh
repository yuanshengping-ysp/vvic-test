# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./dw_rpt_cooperate/dws_plat_live_item_day/dws_plat_live_item_day.sql  
} || {
    echo 'task fail:dws_plat_live_item_day !!!!!!'
	python ./push_ding.py  'dws_plat_live_item_day'
}