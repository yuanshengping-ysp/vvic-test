if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    python ./hive2mysql.py ./dw_rpt_pangge/dws_pangge_item_allot_day/hivetomysql/dws_pangge_item_allot_day.json $today
} || {
    echo 'task fail:dws_pangge_item_allot_day !!!!!!'
	python ./push_ding.py 'dws_pangge_item_allot_day'
}