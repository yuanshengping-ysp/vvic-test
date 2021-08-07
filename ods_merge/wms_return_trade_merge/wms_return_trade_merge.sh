# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./ods_merge/wms_return_trade_merge/wms_return_trade_merge.sql 
} || {
    echo 'task fail:wms_return_trade_merge !!!!!!'
	python ./push_ding.py 'wms_return_trade_merge'
}