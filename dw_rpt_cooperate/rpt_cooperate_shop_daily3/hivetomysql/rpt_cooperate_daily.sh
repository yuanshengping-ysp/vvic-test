if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    python /root/soft/datax/datax/bin/datax.py ./cooperate/rpt_cooperate_shop_daily3/hivetomysql/rpt_cooperate_daily.json -p "-DDATE=${today}"

} || {
    echo 'task fail:rpt_cooperate_daily.json !!!!!!'
	python ./push_ding.py 'rpt_cooperate_daily.json'
}