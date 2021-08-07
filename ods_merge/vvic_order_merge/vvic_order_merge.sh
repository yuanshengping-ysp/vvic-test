# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./ods_merge/vvic_order_merge/vvic_order_merge.sql 
} || {
    echo 'task fail:vvic_order_merge !!!!!!'
	python ./push_ding.py 'vvic_order_merge'
}