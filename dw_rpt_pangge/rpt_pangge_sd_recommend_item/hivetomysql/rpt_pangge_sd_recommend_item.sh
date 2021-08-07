if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    python ./hive2mysql.py ./dw_rpt_pangge/rpt_pangge_sd_recommend_item/hivetomysql/rpt_pangge_sd_recommend_item.json $today
} || {
    echo 'task fail:rpt_pangge_sd_recommend_item !!!!!!'
	python ./push_ding.py 'rpt_pangge_sd_recommend_item'
}