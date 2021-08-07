# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi

{
	hive -hivevar day=$today -f ./dw_rpt_pangge/dws_pangge_shop_allot_daily/dws_pangge_shop_allot_daily.sql	

} || {
    echo 'task fail:dws_pangge_shop_allot_daily !!!!!!'
	python ./push_ding.py 'dws_pangge_shop_allot_daily'
}