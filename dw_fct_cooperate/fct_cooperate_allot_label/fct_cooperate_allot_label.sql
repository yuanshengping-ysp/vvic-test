SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.fct_cooperate_allot_label_temp0 ;
create table if not exists temp.fct_cooperate_allot_label_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.fct_cooperate_allot_label_temp1 ;
create table if not exists temp.fct_cooperate_allot_label_temp1 as select t1.*,t2.stati_date from ods.wms__t_cooperate_label t1 inner join temp.fct_cooperate_allot_label_temp0 t2 on t1.ymd=t2.stati_date ;

drop table if exists temp.fct_cooperate_allot_label_temp2 ;
create table if not exists temp.fct_cooperate_allot_label_temp2 as select t1.stati_date as `date` ,t1.product_label_no ,t3.order_details_id ,t3.sku_id as item_sku_id ,t3.sku_price as item_sku_price ,t3.item_id ,t3.shop_id ,t3.order_no ,t4.user_id from temp.fct_cooperate_allot_label_temp1 t1 left join ods.wms_product_label_subject t2 on t1.product_label_no=t2.product_label_no left join ods.vvic_order_details_subject t3 on t2.ods_order_id=t3.order_details_id left join ods.vvic_order_subject t4 on t3.order_no=t4.order_no ;

insert overwrite table dw.fct_cooperate_allot_label partition(`date`='${hivevar:day}') select product_label_no ,order_details_id ,item_sku_id ,item_sku_price ,item_id ,shop_id ,order_no ,user_id from temp.fct_cooperate_allot_label_temp2 ;

