# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./ods_his/cooperate_shop_his/cooperate_shop_his.sql 
} || {
    echo 'task fail:cooperate_shop_his !!!!!!'
	python ./push_ding.py 'cooperate_shop_his'
}