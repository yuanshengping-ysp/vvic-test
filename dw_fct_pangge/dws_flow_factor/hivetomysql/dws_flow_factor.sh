if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi



{ 
	python ./hive2mysql.py ./dw_fct_pangge/dws_flow_factor/hivetomysql/dws_flow_factor_new.json $today
} || {
	python ./push_ding.py 'dws_flow_factor_new.json'
}