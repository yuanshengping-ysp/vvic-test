if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./dw_rpt_cooperate/dws_recsys_recall_shop_daily/hivetomysql/dws_plat_shop_roi_day.sql 
    python /root/soft/datax/datax/bin/datax.py ./dw_rpt_cooperate/dws_recsys_recall_shop_daily/hivetomysql/dws_plat_shop_roi_day.json -p -DDATE=$today
    python /root/soft/datax/datax/bin/datax.py ./dw_rpt_cooperate/dws_recsys_recall_shop_daily/hivetomysql/dws_plat_shop_roi_day_new.json -p -DDATE=$today
} || {
    echo 'task fail:dws_plat_shop_roi_day !!!!!!'
	python ./push_ding.py 'dws_plat_shop_roi_day.json'
}