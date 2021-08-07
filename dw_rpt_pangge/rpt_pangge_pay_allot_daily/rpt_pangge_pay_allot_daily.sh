# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi

{
	hive -hivevar day=$today -f ./dw_rpt_pangge/rpt_pangge_pay_allot_daily/rpt_pangge_pay_allot_daily.sql	

} || {
    echo 'task fail:rpt_pangge_pay_allot_daily !!!!!!'
	python ./push_ding.py 'rpt_pangge_pay_allot_daily'
}