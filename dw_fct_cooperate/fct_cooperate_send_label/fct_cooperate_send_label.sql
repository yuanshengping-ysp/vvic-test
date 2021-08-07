SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.fct_cooperate_send_label_temp0 ;
create table if not exists temp.fct_cooperate_send_label_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.fct_cooperate_send_label_temp1 ;
create table if not exists temp.fct_cooperate_send_label_temp1 as select t1.*,t2.stati_date from ods.wms__t_pay_record t1 inner join temp.fct_cooperate_send_label_temp0 t2 on t1.ymd=t2.stati_date ;

drop table if exists temp.fct_cooperate_send_label_temp2 ;
create table if not exists temp.fct_cooperate_send_label_temp2 as select t1.stati_date as `date` ,t2.product_label_no ,t4.order_details_id ,t4.sku_id as item_sku_id ,t4.sku_price as item_sku_price ,t2.purchase_price as item_purchase_price ,t4.item_id ,t4.shop_id ,t5.order_no ,t5.user_id ,t2.create_time from temp.fct_cooperate_send_label_temp1 t1 left join ods.wms__t_pay_record_detail t2 on t1.id=t2.pay_id left join ods.wms_product_label_subject t3 on t2.product_label_no=t3.product_label_no left join ods.vvic_order_details_subject t4 on t3.ods_order_id=t4.order_details_id left join ods.vvic_order_subject t5 on t4.order_no=t5.order_no ;

insert overwrite table dw.fct_cooperate_send_label partition(`date`='${hivevar:day}') select product_label_no ,order_details_id ,item_sku_id ,item_sku_price ,item_purchase_price ,item_id ,shop_id ,order_no ,user_id ,create_time from temp.fct_cooperate_send_label_temp2 ;

