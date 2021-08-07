if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    python ./hive2mysql.py ./dw_rpt_pangge/rpt_pangge_sd_shop_sales/hivetomysql/rpt_pangge_sd_shop_sales.json $today
} || {
    echo 'task fail:rpt_pangge_sd_shop_sales !!!!!!'
	python ./push_ding.py 'rpt_pangge_sd_shop_sales'
}