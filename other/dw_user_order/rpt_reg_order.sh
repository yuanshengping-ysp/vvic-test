# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./other/rpt_reg_order/rpt_reg_order.sql 
} || {
    echo 'task fail:rpt_reg_order !!!!!!'
	python ./push_ding.py 'rpt_reg_order'
}