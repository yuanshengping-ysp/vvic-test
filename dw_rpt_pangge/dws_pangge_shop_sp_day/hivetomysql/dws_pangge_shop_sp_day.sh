if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f  ./dw_rpt_pangge/dws_pangge_shop_sp_day/hivetomysql/dws_pangge_shop_sp_day_temp.sql
	python ./hive2mysql.py ./dw_rpt_pangge/dws_pangge_shop_sp_day/hivetomysql/dws_pangge_shop_sp_day_temp.json $today
} || {
    echo 'task fail:dws_pangge_shop_sp_day !!!!!!'
	python ./push_ding.py 'dws_pangge_shop_sp_day'
}