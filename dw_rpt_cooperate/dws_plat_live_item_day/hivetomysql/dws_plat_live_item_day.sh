if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{

    python /root/soft/datax/datax/bin/datax.py ./dw_rpt_cooperate/dws_plat_live_item_day/hivetomysql/dws_plat_live_item_day_new.json -p -DDATE=$today
} || {
    echo 'task fail:dws_plat_live_item_day !!!!!!'
	python ./push_ding.py 'dws_plat_live_item_day'
}