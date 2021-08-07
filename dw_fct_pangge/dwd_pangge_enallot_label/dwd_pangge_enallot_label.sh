# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./dw_fct_pangge/dwd_pangge_enallot_label/dwd_pangge_enallot_label.sql 
} || {
    echo 'task fail:dwd_pangge_enallot_label !!!!!!'
	python ./push_ding.py 'dwd_pangge_enallot_label'
}