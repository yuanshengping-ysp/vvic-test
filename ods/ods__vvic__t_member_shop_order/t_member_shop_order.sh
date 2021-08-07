today=`date -d -1days '+%Y-%m-%d'`
hive -e "drop table if exists ods.vvic__t_member_shop_order"
hive -f ./ods/ods__vvic__t_member_shop_order/t_member_shop_order.sql 
python /root/soft/datax/datax/bin/datax.py ./ods/ods__vvic__t_member_shop_order/t_member_shop_order.json -p -DDATE=$today