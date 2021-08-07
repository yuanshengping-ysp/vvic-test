today=`date -d -1days '+%Y-%m-%d'`
hive -e "drop table if exists ods.t_shop"
hive -f ./ods/ods__t_shop/t_shop.sql 
python /root/soft/datax/datax/bin/datax.py ./ods/ods__t_shop/t_shop.json -p -DDATE=$today