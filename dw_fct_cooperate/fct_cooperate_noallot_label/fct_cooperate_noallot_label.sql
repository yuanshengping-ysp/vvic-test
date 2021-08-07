SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.fct_cooperate_noallot_label_temp0 ;
create table if not exists temp.fct_cooperate_noallot_label_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.fct_cooperate_noallot_label_temp1 ;
create table if not exists temp.fct_cooperate_noallot_label_temp1 as select t.*  ,(case when t.next_time is null then current_date() else substring(t.next_time,1,10) end) as next_date from (select * ,lead(current_life_status,1) over(partition by product_label_no order by create_time) as next_life_status ,lead(create_time,1) over(partition by product_label_no order by create_time) as next_time from ods.wms__wms_product_label_log where create_time>=date_sub('${hivevar:day}',15) and create_time<date_add('${hivevar:day}',1)) t where t.current_life_status=3 and t.origin_life_status=0 ;

drop table if exists temp.fct_cooperate_noallot_label_temp2 ;
create table if not exists temp.fct_cooperate_noallot_label_temp2 as select t1.* ,t2.order_details_id ,t2.item_sku_id ,t2.item_sku_price ,t2.item_id ,t2.shop_id ,t2.order_no ,t2.user_id ,t2.item_sn from temp.fct_cooperate_noallot_label_temp1 t1 left join dw.dim_wms_product_label t2 on t1.product_label_no=t2.product_label_no ;

drop table if exists temp.fct_cooperate_noallot_label_temp3 ;
create table if not exists temp.fct_cooperate_noallot_label_temp3 as select t1.* from temp.fct_cooperate_noallot_label_temp2 t1 inner join (select * from dw.fct_cooperate_shop where `date`>=date_sub('${hivevar:day}',15) and `date`<='${hivevar:day}' and coo_shop_num=1) t2 on t1.shop_id=t2.shop_id and t1.`date`=t2.`date` ;

drop table if exists temp.fct_cooperate_noallot_label_temp4 ;
create table if not exists temp.fct_cooperate_noallot_label_temp4 as select t1.product_label_no ,t1.order_details_id ,t1.item_sku_id ,t1.item_sku_price ,t1.item_id ,t1.shop_id ,t1.order_no ,t1.user_id ,t1.item_sn ,t2.stati_date as `date` from temp.fct_cooperate_noallot_label_temp3 t1 ,temp.fct_cooperate_noallot_label_temp0 t2 where t1.`date`<=t2.stati_date and t1.next_date>t2.stati_date ;

insert overwrite table dw.fct_cooperate_noallot_label partition(`date`='${hivevar:day}') select product_label_no ,order_details_id ,item_sku_id ,item_sku_price ,item_id ,shop_id ,order_no ,user_id from temp.fct_cooperate_noallot_label_temp4 ;

