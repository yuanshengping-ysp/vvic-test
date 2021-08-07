# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./dw_fct_cooperate/fct_cooperate_ship_label/fct_cooperate_ship_label.sql 
} || {
    echo 'task fail:fct_cooperate_ship_label !!!!!!'
	python ./push_ding.py 'fct_cooperate_ship_label'
}