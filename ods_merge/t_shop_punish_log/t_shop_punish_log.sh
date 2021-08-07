if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi



{
    hive -e 'truncate table ods.t_shop_punish_log;'
	python /root/soft/datax/datax/bin/datax.py ./ods_merge/t_shop_punish_log/t_shop_punish_log.json -p -DDATE=$today
} || {
	python ./push_ding.py 't_shop_punish_log.json'
}