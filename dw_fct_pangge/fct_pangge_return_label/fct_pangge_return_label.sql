SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.fct_pangge_return_label_temp0 ;
create table if not exists temp.fct_pangge_return_label_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.fct_pangge_return_label_temp1 ;
create table if not exists temp.fct_pangge_return_label_temp1 as select substring(create_time,1,10) as `date` ,create_time ,0 as `type` ,return_label_no ,return_trade_id ,return_order_id ,buy_price as item_buy_price ,return_price as item_return_price ,1 as return_item_num from ods.wms_return_label_merge where create_time>='${hivevar:day}' and create_time<date_add('${hivevar:day}',1) union all select substring(update_time,1,10) as `date` ,update_time as create_time ,1 as `type` ,return_label_no ,return_trade_id ,return_order_id ,buy_price as item_buy_price ,return_price as item_return_price ,1 as return_item_num from ods.wms_return_label_merge where return_label_status=3 and update_time>='${hivevar:day}' and update_time<date_add('${hivevar:day}',1);

drop table if exists temp.fct_pangge_return_label_temp2 ;
create table if not exists temp.fct_pangge_return_label_temp2 as select t1.* ,t2.return_type ,t2.return_trade_no ,t3.order_no ,t3.order_details_id ,t2.city_market_id ,t3.shop_id ,t3.item_id ,t3.sku_id as item_sku_id ,t4.user_id ,t3.sku_price as item_sku_price from temp.fct_pangge_return_label_temp1 t1 left join ods.wms_return_trade_merge t2 on t1.return_trade_id=t2.return_trade_id left join ods.ods__vvic_order_refund_details t3 on t1.return_label_no=t3.label_no left join ods.ods__vvic_order_refund t4 on t3.refund_no=t4.refund_no ;

drop table if exists temp.fct_pangge_return_label_temp3 ;
create table if not exists temp.fct_pangge_return_label_temp3 as select t1.`date` ,t1.city_market_id ,t1.create_time ,t1.user_id ,t1.return_label_no ,t1.return_trade_no ,t1.return_type ,t1.`type` ,t1.shop_id ,t1.item_id ,t1.order_details_id ,t1.order_no ,t1.item_sku_id ,t1.item_sku_price ,t1.item_buy_price ,t1.item_return_price ,t1.return_item_num from temp.fct_pangge_return_label_temp2 t1 inner join temp.fct_pangge_return_label_temp0 t2 on t1.`date`=t2.stati_date ;

insert overwrite table dw.fct_pangge_return_label partition(`date`='${hivevar:day}') select city_market_id ,create_time ,user_id ,return_label_no ,return_trade_no ,return_type ,`type` ,shop_id ,item_id ,order_details_id ,order_no ,item_sku_id ,item_sku_price ,item_buy_price ,item_return_price ,return_item_num from temp.fct_pangge_return_label_temp3 ;
