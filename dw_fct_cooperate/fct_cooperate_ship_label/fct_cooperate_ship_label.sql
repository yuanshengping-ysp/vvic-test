SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.fct_cooperate_ship_label_temp0 ;
create table if not exists temp.fct_cooperate_ship_label_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.fct_cooperate_ship_label_temp1 ;
create table if not exists temp.fct_cooperate_ship_label_temp1 as select t1.* from (select *,substring(packing_time,1,10) as ship_date from ods.wms_product_label_subject where packing_time>='${hivevar:day}' and packing_time<date_add('${hivevar:day}',1) ) t1 inner join temp.fct_cooperate_ship_label_temp0 t2 on t1.ship_date=t2.stati_date ;

drop table if exists temp.fct_cooperate_ship_label_temp2 ;
create table if not exists temp.fct_cooperate_ship_label_temp2 as select t1.product_label_no ,t2.order_details_id ,t2.item_sku_id ,t2.item_sku_price ,t2.item_id ,t2.shop_id ,t2.order_no ,t2.user_id ,t2.pay_time ,t1.packing_time as ship_time ,t1.ship_date as `date` from temp.fct_cooperate_ship_label_temp1 t1 inner join (select distinct order_details_id ,item_sku_id ,item_sku_price ,order_no ,item_id ,user_id ,shop_id ,pay_time from dw.fct_cooperate_pay_label) t2 on cast(t1.ods_order_id as string)=cast(t2.order_details_id as string) ;

insert overwrite table dw.fct_cooperate_ship_label partition(`date`='${hivevar:day}') select product_label_no ,order_details_id ,item_sku_id ,item_sku_price ,item_id ,shop_id ,order_no ,user_id ,pay_time ,ship_time from temp.fct_cooperate_ship_label_temp2 ;

