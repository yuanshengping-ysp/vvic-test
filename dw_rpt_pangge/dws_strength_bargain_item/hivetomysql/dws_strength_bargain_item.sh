if [ $# == 1 ]
then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    python /root/soft/datax/datax/bin/datax.py ./dw_rpt_pangge/dws_strength_bargain_item/hivetomysql/dws_strength_bargain_item.json -p -DDATE=$today
} || {
    echo 'task fail:dws_strength_bargain_item !!!!!!'
	python otbak/push_ding.py 'dws_strength_bargain_item'
}