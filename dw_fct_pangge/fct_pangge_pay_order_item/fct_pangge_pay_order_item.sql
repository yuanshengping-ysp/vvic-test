SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.fct_pangge_pay_order_item_temp0 ;
create table if not exists temp.fct_pangge_pay_order_item_temp0 as select explode(split('${hivevar:day}',',')) as stati_date ;

drop table if exists temp.fct_pangge_pay_order_item_temp1 ;
create table if not exists temp.fct_pangge_pay_order_item_temp1 as select t1.pay_time ,t2.city_market_id ,t1.order_no ,t2.order_type ,t1.user_id ,t1.pay_item_money ,t1.pay_order_money ,substring(t1.pay_time,1,10) as `date` from (select create_time as pay_time ,order_no ,user_id ,goods_amount as pay_item_money ,amount as pay_order_money from ods.vvic_user_trade_subject where create_time>='${hivevar:day}' and create_time<date_add('${hivevar:day}',1) and trade_type=1 and trade_name='搜款网下单') t1 inner join (select * from ods.vvic_order_merge where order_type<>1 ) t2 on t1.order_no=t2.order_no  ;

drop table if exists temp.fct_pangge_pay_order_item_temp2 ;
create table if not exists temp.fct_pangge_pay_order_item_temp2 as select t1.* from temp.fct_pangge_pay_order_item_temp1 t1 inner join temp.fct_pangge_pay_order_item_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.fct_pangge_pay_order_item_temp3 ;
create table if not exists temp.fct_pangge_pay_order_item_temp3 as select t1.pay_time ,t1.city_market_id ,t1.order_type ,t1.order_no ,t2.order_details_id ,t1.user_id ,t1.pay_order_money ,t1.pay_item_money ,t2.shop_id ,t2.item_id ,t2.sku_id as item_sku_id ,t2.sku_price as item_sku_price ,t1.`date` from temp.fct_pangge_pay_order_item_temp2 t1 left join ods.vvic_order_details_merge t2 on t1.order_no=t2.order_no ;

drop table if exists temp.fct_pangge_pay_order_item_temp4 ;
create table if not exists temp.fct_pangge_pay_order_item_temp4 as select t1.order_details_id ,min(t2.create_time) as min_create_time from temp.fct_pangge_pay_order_item_temp3 t1 inner join ods.wms_product_label_merge t2 on t1.order_details_id=t2.ods_order_id group by t1.order_details_id ;

drop table if exists temp.fct_pangge_pay_order_item_temp5 ;
create table if not exists temp.fct_pangge_pay_order_item_temp5 as select t1.order_details_id ,t2.create_time ,count(1) as num from temp.fct_pangge_pay_order_item_temp3 t1 inner join ods.wms_product_label_merge t2 on t1.order_details_id=t2.ods_order_id group by t1.order_details_id ,t2.create_time ;

drop table if exists temp.fct_pangge_pay_order_item_temp6 ;
create table if not exists temp.fct_pangge_pay_order_item_temp6 as select t1.* ,t2.min_create_time ,unix_timestamp(t1.create_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(t2.min_create_time,'yyyy-MM-dd HH:mm:ss') as seconds from temp.fct_pangge_pay_order_item_temp5 t1 left join temp.fct_pangge_pay_order_item_temp4 t2 on t1.order_details_id=t2.order_details_id ;

drop table if exists temp.fct_pangge_pay_order_item_temp7 ;
create table if not exists temp.fct_pangge_pay_order_item_temp7 as select order_details_id ,sum(num) as num from temp.fct_pangge_pay_order_item_temp6 where seconds<=15 group by order_details_id ;

insert overwrite table dw.fct_pangge_pay_order_item partition(`date`='${hivevar:day}') select t1.pay_time ,t1.city_market_id ,t1.user_id ,t1.order_type ,t1.order_no ,t1.order_details_id ,t1.shop_id ,t1.item_id ,t1.item_sku_id ,t1.item_sku_price ,t2.num*t1.item_sku_price as item_sku_money ,t1.pay_item_money ,t1.pay_order_money ,t2.num as pay_item_num from temp.fct_pangge_pay_order_item_temp3 t1 left join temp.fct_pangge_pay_order_item_temp7 t2 on t1.order_details_id=t2.order_details_id ;

