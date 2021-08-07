SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;



drop table if exists temp.dim_wms_product_label_temp0 ;
create table if not exists temp.dim_wms_product_label_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.dim_wms_product_label_temp1 ;
create table if not exists temp.dim_wms_product_label_temp1 as select t1.* from (select * from ods.wms_product_label_merge where create_time>=date_sub('${hivevar:day}',60) and create_time<date_add('${hivevar:day}',1)) t1 inner join (select distinct ods_order_id from ods.wms_product_label_merge where create_time>='${hivevar:day}' and create_time<date_add('${hivevar:day}',1)) t2 on t1.ods_order_id=t2.ods_order_id ;

drop table if exists temp.dim_wms_product_label_temp2 ;
create table if not exists temp.dim_wms_product_label_temp2 as select t1.product_label_no ,t1.ods_order_id ,t1.create_time ,t1.qc_status ,t1.qc_time ,t2.order_details_id ,t2.order_id ,t2.order_no ,t2.item_id ,t2.item_sn ,t2.sku_id as item_sku_id ,t2.sku_price as item_sku_price ,t2.sku_color as item_sku_color ,t2.sku_size as item_sku_size ,t2.subject_id ,t2.shop_id ,(case when t3.user_id is null then t2.user_id else t3.user_id end) as user_id ,t3.order_type ,t4.create_time as pay_time ,(case when t3.city_market_id is null then t2.city_market_id else t3.city_market_id end) as city_market_id ,(case when t1.qc_status=9 then t1.qc_time else null end) as qc_time_new from temp.dim_wms_product_label_temp1 t1 left join (select * from ods.vvic_order_details_merge where (create_time>=date_sub('${hivevar:day}',60) and create_time<date_add('${hivevar:day}',1)) or create_time is null or create_time='' ) t2 on t1.ods_order_id=t2.order_details_id left join (select * from ods.vvic_order_merge where (create_time>=date_sub('${hivevar:day}',60) and create_time<date_add('${hivevar:day}',1)) or create_time is null or create_time='' ) t3 on t2.order_no=t3.order_no left join (select * from ods.vvic_user_trade where trade_type=1 and trade_name='搜款网下单' and ((create_time>=date_sub('${hivevar:day}',60) and create_time<date_add('${hivevar:day}',1)) or create_time is null or create_time='') ) t4 on t3.order_no=t4.order_no ;

drop table if exists temp.dim_wms_product_label_temp3 ;
create table if not exists temp.dim_wms_product_label_temp3 as select t1.* ,t2.first_create_time from temp.dim_wms_product_label_temp2 t1 left join (select ods_order_id ,min(create_time) as first_create_time from temp.dim_wms_product_label_temp2 group by ods_order_id) t2 on t1.ods_order_id=t2.ods_order_id ;

drop table if exists temp.dim_wms_product_label_temp4 ;
create table if not exists temp.dim_wms_product_label_temp4 as select * ,lag(qc_time_new,1) over (partition by order_details_id order by create_time) as qc_create_time from temp.dim_wms_product_label_temp3 ;

drop table if exists temp.dim_wms_product_label_temp5 ;
create table if not exists temp.dim_wms_product_label_temp5 as select * ,(case when unix_timestamp(create_time)-unix_timestamp(first_create_time) between 0 and 15 then 0 when unix_timestamp(create_time)-unix_timestamp(qc_create_time) between 0 and 5 then 2 else 1 end) as label_type ,substring(create_time,1,10) as `date` from temp.dim_wms_product_label_temp4 ;

drop table if exists temp.dim_wms_product_label_temp6 ;
create table if not exists temp.dim_wms_product_label_temp6 as select t1.* from temp.dim_wms_product_label_temp5 t1 inner join temp.dim_wms_product_label_temp0 t2 on t1.`date`=t2.stati_date ;

insert overwrite table dw.dim_wms_product_label partition(`date`='${hivevar:day}') select product_label_no ,create_time ,label_type ,order_details_id ,order_id,order_no ,order_type ,item_sn ,item_sku_id ,item_sku_color ,item_sku_size ,item_sku_price ,item_id ,shop_id ,user_id ,city_market_id ,subject_id ,pay_time from temp.dim_wms_product_label_temp6 ;

