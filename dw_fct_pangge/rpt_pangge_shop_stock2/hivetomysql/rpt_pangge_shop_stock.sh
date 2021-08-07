if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -3days '+%Y-%m-%d'`
fi

{
    hive -hivevar day=$today -f   ./dw_fct_pangge/rpt_pangge_shop_stock2/hivetomysql/rpt_pangge_shop_stock2_temp.sql 
	python ./hive2mysql.py ./dw_fct_pangge/rpt_pangge_shop_stock2/hivetomysql/rpt_pangge_shop_stock.json -p -DDATE=$today
} || {
    echo 'task fail:rpt_pangge_shop_stock !!!!!!'
	python otbak/push_ding.py 'rpt_pangge_shop_stock'
}