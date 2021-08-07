SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.fct_cooperate_pay_label_temp0 ;
create table if not exists temp.fct_cooperate_pay_label_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.fct_cooperate_pay_label_temp1 ;
create table if not exists temp.fct_cooperate_pay_label_temp1 as select t1.pay_time ,t1.order_no ,t2.order_type ,substring(t1.pay_time,1,10) as `date` ,t2.user_id from (select create_time as pay_time ,order_no from ods.vvic_user_trade_subject where create_time>='${hivevar:day}' and create_time<date_add('${hivevar:day}',1) and trade_type=1 and trade_name='搜款网下单') t1 inner join (select * from ods.vvic_order_subject where order_type<>1 ) t2 on t1.order_no=t2.order_no  ;

drop table if exists temp.fct_cooperate_pay_label_temp2 ;
create table if not exists temp.fct_cooperate_pay_label_temp2 as select t1.* from temp.fct_cooperate_pay_label_temp1 t1 inner join temp.fct_cooperate_pay_label_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.fct_cooperate_pay_label_temp3 ;
create table if not exists temp.fct_cooperate_pay_label_temp3 as select t1.pay_time ,t1.order_type ,t1.order_no ,t2.order_details_id ,t2.sku_price as item_sku_price ,t2.sku_id as item_sku_id ,t2.item_id ,t2.shop_id ,t3.product_label_no ,t3.create_time ,t1.user_id ,t1.`date` from temp.fct_cooperate_pay_label_temp2 t1 left join ods.vvic_order_details_subject t2 on t1.order_no=t2.order_no left join ods.wms_product_label_subject t3 on cast(t2.order_details_id as string)=cast(t3.ods_order_id as string) ;

drop table if exists temp.fct_cooperate_pay_label_temp4 ;
create table if not exists temp.fct_cooperate_pay_label_temp4 as select * ,first_value(create_time) over (partition by order_details_id order by create_time) as min_create_time from temp.fct_cooperate_pay_label_temp3 ;

drop table if exists temp.fct_cooperate_pay_label_temp5 ;
create table if not exists temp.fct_cooperate_pay_label_temp5 as select * ,unix_timestamp(create_time)-unix_timestamp(min_create_time) as seconds from temp.fct_cooperate_pay_label_temp4 ;

drop table if exists temp.fct_cooperate_pay_label_temp6 ;
create table if not exists temp.fct_cooperate_pay_label_temp6 as select * from temp.fct_cooperate_pay_label_temp5 where seconds<=10 ;

drop table if exists temp.fct_cooperate_pay_label_temp7 ;
create table if not exists temp.fct_cooperate_pay_label_temp7 as select t1.* from temp.fct_cooperate_pay_label_temp6 t1 inner join dw.fct_cooperate_shop t2 on t1.`date`=t2.`date` and t1.shop_id=t2.shop_id ;

insert overwrite table dw.fct_cooperate_pay_label partition(`date`='${hivevar:day}') select product_label_no ,order_details_id ,item_sku_id ,item_sku_price ,item_id ,shop_id ,order_no ,user_id ,create_time ,pay_time from temp.fct_cooperate_pay_label_temp7;
