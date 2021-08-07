# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f  ./dw_rpt_pangge/dws_strength_bargain_item/dws_strength_bargain_item.sql  

} || {
    echo 'task fail:dws_strength_bargain_item !!!!!!'
	python otbak/push_ding.py 'dws_strength_bargain_item'
}