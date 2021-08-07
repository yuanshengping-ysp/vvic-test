SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.fct_pangge_stock_item_temp0 ;
create table if not exists temp.fct_pangge_stock_item_temp0 as select explode(split(CONCAT(date_sub('${hivevar:day}',1),',','${hivevar:day}'),',')) as stati_date ;

drop table if exists temp.fct_pangge_stock_item_temp1 ;
create table if not exists temp.fct_pangge_stock_item_temp1 as select product_label_no ,event_type ,create_time ,0 as `type` from ods.wms__wms_product_label_log where create_time>=date_sub('${hivevar:day}',1) and event_type in ('011','012','013') and trigger_event regexp('分配次数：1') union all select product_label_no ,event_type ,create_time ,1 as `type` from ods.wms__wms_product_label_log where create_time>=date_sub('${hivevar:day}',1) and event_type in ('04') ;

drop table if exists temp.fct_pangge_stock_item_temp2 ;
create table if not exists temp.fct_pangge_stock_item_temp2 as select * ,lead(create_time,1) over (partition by product_label_no order by create_time) as receive_time ,substring(create_time,1,10) as `date` from temp.fct_pangge_stock_item_temp1 ;

drop table if exists temp.fct_pangge_stock_item_temp3 ;
create table if not exists temp.fct_pangge_stock_item_temp3 as select t1.* ,(unix_timestamp(t1.receive_time)-unix_timestamp(t1.create_time))*1.0/86400 as period from temp.fct_pangge_stock_item_temp2 t1 inner join temp.fct_pangge_stock_item_temp0 t2 on t1.`date`=t2.stati_date where t1.`type`=0  ;

drop table if exists temp.fct_pangge_stock_item_temp4 ;
create table if not exists temp.fct_pangge_stock_item_temp4 as select `date` ,product_label_no ,count(1) as allot_item_num ,count(case when period<=2 then 1 else null end) as receive_item_num_2d from temp.fct_pangge_stock_item_temp3 group by `date` ,product_label_no ;

drop table if exists temp.fct_pangge_stock_item_temp5 ;
create table if not exists temp.fct_pangge_stock_item_temp5 as select t1.* ,t2.order_details_id ,t2.item_sku_id ,t2.item_sku_price ,t2.item_id ,t2.city_market_id ,t2.shop_id ,t2.order_no ,t2.user_id from temp.fct_pangge_stock_item_temp4 t1 left join (select product_label_no ,city_market_id ,order_details_id ,item_sku_id ,item_sku_price ,order_no ,item_id ,user_id ,shop_id ,order_type ,label_type from dw.dim_wms_product_label where `date`>=date_sub('${hivevar:day}',30) and `date`<='${hivevar:day}' ) t2 on t1.product_label_no=t2.product_label_no where t2.order_type=0 and t2.label_type in (0,1) ;

drop table if exists temp.fct_pangge_stock_item_temp6 ;
create table if not exists temp.fct_pangge_stock_item_temp6 as select `date` ,city_market_id ,order_no ,order_details_id ,item_sku_id ,item_sku_price ,item_id ,shop_id ,user_id ,sum(allot_item_num) as allot_item_num ,sum(receive_item_num_2d) as receive_item_num_2d from temp.fct_pangge_stock_item_temp5 group by `date` ,city_market_id ,order_no ,order_details_id ,item_sku_id ,item_sku_price ,item_id ,shop_id ,user_id ;

insert overwrite table dw.fct_pangge_stock_item partition(`date`) select city_market_id ,order_no ,order_details_id ,item_sku_id ,item_sku_price ,item_id ,shop_id ,user_id ,allot_item_num ,receive_item_num_2d ,`date` from temp.fct_pangge_stock_item_temp6 where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' ;

