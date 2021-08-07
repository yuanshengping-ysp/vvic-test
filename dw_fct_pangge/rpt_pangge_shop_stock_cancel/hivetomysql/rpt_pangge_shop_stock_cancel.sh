if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -3days '+%Y-%m-%d'`
fi

{
    python ./hive2mysql.py ./dw_fct_pangge/rpt_pangge_shop_stock_cancel/hivetomysql/rpt_pangge_shop_stock_cancel.json $today
} || {
    echo 'task fail:rpt_pangge_shop_stock_cancel !!!!!!'
	python ./push_ding.py 'rpt_pangge_shop_stock_cancel'
}