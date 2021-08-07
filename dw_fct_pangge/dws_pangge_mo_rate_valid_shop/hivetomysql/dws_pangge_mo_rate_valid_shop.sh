if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi



{
    hive -hivevar day=$today -f   ./dw_fct_pangge/dws_pangge_mo_rate_valid_shop/hivetomysql/dws_pangge_mo_rate_valid_shop.sql  
	python ./hive2mysql.py ./dw_fct_pangge/dws_pangge_mo_rate_valid_shop/hivetomysql/dws_pangge_mo_rate_valid_shop_new.json $today
} || {
	python ./push_ding.py 'dws_pangge_mo_rate_valid_shop_new.json'
}