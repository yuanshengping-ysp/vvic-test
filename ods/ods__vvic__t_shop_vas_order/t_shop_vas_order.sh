today=`date -d -1days '+%Y-%m-%d'`
hive -e "drop table if exists ods.vvic__t_shop_vas_order"
hive -f ./ods/ods__vvic__t_shop_vas_order/t_shop_vas_order.sql 
python /root/soft/datax/datax/bin/datax.py ./ods/ods__vvic__t_shop_vas_order/t_shop_vas_order.json -p -DDATE=$today