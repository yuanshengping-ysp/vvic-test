# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./other/rpt_user/rpt_user.sql 
} || {
    echo 'task fail:rpt_user !!!!!!'
	python ./push_ding.py 'rpt_user'
}