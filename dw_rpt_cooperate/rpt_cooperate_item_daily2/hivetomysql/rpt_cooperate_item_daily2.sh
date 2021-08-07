if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    python ./hive2mysql.py ./cooperate/rpt_cooperate_item_daily2/hivetomysql/rpt_cooperate_item_daily2.json -p $today

} || {
    echo 'task fail:rpt_cooperate_item_daily2.json !!!!!!'
	python ./push_ding.py 'rpt_cooperate_item_daily2.json'
}