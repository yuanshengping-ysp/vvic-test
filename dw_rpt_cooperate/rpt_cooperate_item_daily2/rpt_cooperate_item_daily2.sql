SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.fct_cooperate_noallot_label_temp0 ;
create table if not exists temp.fct_cooperate_noallot_label_temp0 as select explode(split(CONCAT(date_sub('${hivevar:day}',1),',','${hivevar:day}'),',')) as stati_date ;

drop table if exists temp.fct_cooperate_noallot_label_temp1 ;
create table if not exists temp.fct_cooperate_noallot_label_temp1 as select t.*  ,(case when t.next_time is null then current_date() else substring(t.next_time,1,10) end) as next_date from (select * ,lead(current_life_status,1) over(partition by product_label_no order by create_time) as next_life_status ,lead(create_time,1) over(partition by product_label_no order by create_time) as next_time from ods.wms__wms_product_label_log where create_time>=date_sub('${hivevar:day}',16) and create_time<date_add('${hivevar:day}',1)) t where t.current_life_status=3 and t.origin_life_status=0 ;

drop table if exists temp.fct_cooperate_noallot_label_temp2 ;
create table if not exists temp.fct_cooperate_noallot_label_temp2 as select t1.* ,t2.order_details_id ,t2.item_sku_id ,t2.item_sku_price ,t2.item_id ,t2.shop_id ,t2.order_no ,t2.user_id ,t2.item_sn from temp.fct_cooperate_noallot_label_temp1 t1 left join dw.dim_wms_product_label t2 on t1.product_label_no=t2.product_label_no ;

drop table if exists temp.fct_cooperate_noallot_label_temp3 ;
create table if not exists temp.fct_cooperate_noallot_label_temp3 as select t1.* from temp.fct_cooperate_noallot_label_temp2 t1 inner join (select * from dw.fct_cooperate_shop where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}' and coo_shop_num=1) t2 on t1.shop_id=t2.shop_id and t1.`date`=t2.`date` ;

drop table if exists temp.fct_cooperate_noallot_label_temp4 ;
create table if not exists temp.fct_cooperate_noallot_label_temp4 as select t1.product_label_no ,t1.order_details_id ,t1.item_sku_id ,t1.item_sku_price ,t1.item_id ,t1.shop_id ,t1.order_no ,t1.user_id ,t1.item_sn ,t2.stati_date as `date` from temp.fct_cooperate_noallot_label_temp3 t1 ,temp.fct_cooperate_noallot_label_temp0 t2 where t1.`date`<=t2.stati_date and t1.next_date>t2.stati_date ;

insert overwrite table dw.fct_cooperate_noallot_label partition(`date`) select product_label_no ,order_details_id ,item_sku_id ,item_sku_price ,item_id ,shop_id ,order_no ,user_id ,`date` from temp.fct_cooperate_noallot_label_temp4;

drop table if exists temp.rpt_cooperate_item_daily2_temp0 ;
create table if not exists temp.rpt_cooperate_item_daily2_temp0 as select explode(split(CONCAT(date_sub('${hivevar:day}',1),',','${hivevar:day}'),',')) as stati_date ;

drop table if exists temp.rpt_cooperate_item_daily2_temp1 ;
create table if not exists temp.rpt_cooperate_item_daily2_temp1 as select `date` ,shop_id ,item_sku_id ,1 as `type` ,count(1) as item_num from dw.fct_cooperate_allot_label where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' group by `date` ,shop_id ,item_sku_id ;

drop table if exists temp.rpt_cooperate_item_daily2_temp2 ;
create table if not exists temp.rpt_cooperate_item_daily2_temp2 as select `date` ,shop_id ,item_sku_id ,2 as `type` ,count(1) as item_num from dw.fct_cooperate_send_label where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' group by `date` ,shop_id ,item_sku_id ;

drop table if exists temp.rpt_cooperate_item_daily2_temp3 ;
create table if not exists temp.rpt_cooperate_item_daily2_temp3 as select `date` ,shop_id ,item_sku_id ,5 as `type` ,count(1) as item_num from dw.fct_cooperate_noallot_label where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' group by `date` ,shop_id ,item_sku_id ;

drop table if exists temp.rpt_cooperate_item_daily2_temp4 ;
create table if not exists temp.rpt_cooperate_item_daily2_temp4 as select `date` ,shop_id ,cast(item_sku_id as string) as item_sku_id ,3 as `type` ,return_item_num as item_num from dw.fct_cooperate_return_label where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' union all select `date` ,shop_id ,cast(item_sku_id as string) as item_sku_id ,4 as `type` ,return_success_item_num as item_num from dw.fct_cooperate_return_label where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' union all select `date` ,shop_id ,item_sku_id ,`type` ,item_num from temp.rpt_cooperate_item_daily2_temp1 union all select `date` ,shop_id ,item_sku_id ,`type` ,item_num from temp.rpt_cooperate_item_daily2_temp2 union all select `date` ,shop_id ,item_sku_id ,`type` ,item_num from temp.rpt_cooperate_item_daily2_temp3 ;

drop table if exists temp.rpt_cooperate_item_daily2_temp5 ;
create table if not exists temp.rpt_cooperate_item_daily2_temp5 as select t1.* from temp.rpt_cooperate_item_daily2_temp4 t1 inner join temp.rpt_cooperate_item_daily2_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_cooperate_item_daily2_temp6 ;
create table if not exists temp.rpt_cooperate_item_daily2_temp6 as select `date` ,shop_id ,item_sku_id ,sum(case when `type`=5 then item_num else 0 end) as noallot_item_num ,sum(case when `type`=1 then item_num else 0 end) as allot_item_num ,sum(case when `type`=2 then item_num else 0 end) as send_item_num ,sum(case when `type`=3 then item_num else 0 end) as return_item_num ,sum(case when `type`=4 then item_num else 0 end) as return_success_item_num from temp.rpt_cooperate_item_daily2_temp5 group by `date` ,shop_id ,item_sku_id ;

drop table if exists temp.rpt_cooperate_item_daily2_temp7 ;
create table if not exists temp.rpt_cooperate_item_daily2_temp7 as select t1.* ,t2.name as shop_name ,t2.bid ,t3.name as market_name ,t4.lower_price ,t5.item_id from temp.rpt_cooperate_item_daily2_temp6 t1 left join ods.t_shop t2 on t1.shop_id=t2.id left join (select distinct bid,name from ods.t_market) t3 on t2.bid=t3.bid left join (select distinct `date` ,shop_id ,lower_price from dw.fct_cooperate_shop) t4 on t1.`date`=t4.`date` and t1.shop_id=t4.shop_id left join (select distinct item_id,sku_id from ods.vvic_order_details_merge ) t5 on t1.item_sku_id=cast(t5.sku_id as string) ;


insert overwrite table rpt.rpt_cooperate_item_daily2 partition(`date`) select shop_id ,shop_name ,bid ,market_name ,item_id ,item_sku_id ,lower_price ,noallot_item_num ,allot_item_num ,send_item_num ,return_item_num ,return_success_item_num ,`date` from temp.rpt_cooperate_item_daily2_temp7;

