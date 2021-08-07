SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.rpt_cooperate_item_daily_temp0 ;
create table if not exists temp.rpt_cooperate_item_daily_temp0 as select explode(split(CONCAT(date_sub('${hivevar:day}',1),',','${hivevar:day}'),',')) as stati_date ;

drop table if exists temp.rpt_cooperate_item_daily_temp1 ;
create table if not exists temp.rpt_cooperate_item_daily_temp1 as select `date` ,shop_id ,item_sku_id ,1 as `type` ,count(1) as item_num from dw.fct_cooperate_allot_label where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' group by `date` ,shop_id ,item_sku_id ;

drop table if exists temp.rpt_cooperate_item_daily_temp2 ;
create table if not exists temp.rpt_cooperate_item_daily_temp2 as select `date` ,shop_id ,item_sku_id ,2 as `type` ,count(1) as item_num from dw.fct_cooperate_send_label where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' group by `date` ,shop_id ,item_sku_id ;

drop table if exists temp.rpt_cooperate_item_daily_temp3 ;
create table if not exists temp.rpt_cooperate_item_daily_temp3 as select `date` ,shop_id ,cast(item_sku_id as string) as item_sku_id ,3 as `type` ,return_item_num as item_num from dw.fct_cooperate_return_label where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' union all select `date` ,shop_id ,cast(item_sku_id as string) as item_sku_id ,4 as `type` ,return_success_item_num as item_num from dw.fct_cooperate_return_label where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' union all select `date` ,shop_id ,item_sku_id ,`type` ,item_num from temp.rpt_cooperate_item_daily_temp1 union all select `date` ,shop_id ,item_sku_id ,`type` ,item_num from temp.rpt_cooperate_item_daily_temp2 ;

drop table if exists temp.rpt_cooperate_item_daily_temp4 ;
create table if not exists temp.rpt_cooperate_item_daily_temp4 as select t1.* from temp.rpt_cooperate_item_daily_temp3 t1 inner join temp.rpt_cooperate_item_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_cooperate_item_daily_temp5 ;
create table if not exists temp.rpt_cooperate_item_daily_temp5 as select `date` ,shop_id ,item_sku_id ,sum(case when `type`=1 then item_num else 0 end) as allot_item_num ,sum(case when `type`=2 then item_num else 0 end) as send_item_num ,sum(case when `type`=3 then item_num else 0 end) as return_item_num ,sum(case when `type`=4 then item_num else 0 end) as return_success_item_num from temp.rpt_cooperate_item_daily_temp4 group by `date` ,shop_id ,item_sku_id ;

drop table if exists temp.rpt_cooperate_item_daily_temp6 ;
create table if not exists temp.rpt_cooperate_item_daily_temp6 as select t1.* ,t2.name as shop_name ,t2.bid ,t3.name as market_name ,t4.lower_price from temp.rpt_cooperate_item_daily_temp5 t1 left join ods.t_shop t2 on t1.shop_id=t2.id left join (select distinct bid,name from ods.t_market) t3 on t2.bid=t3.bid left join (select distinct `date` ,shop_id ,lower_price from dw.fct_cooperate_shop) t4 on t1.`date`=t4.`date` and t1.shop_id=t4.shop_id ;

insert overwrite table rpt.rpt_cooperate_item_daily partition(`date`) select shop_id ,shop_name ,bid ,market_name ,item_sku_id ,lower_price ,allot_item_num ,send_item_num ,return_item_num ,return_success_item_num ,`date` from temp.rpt_cooperate_item_daily_temp6 where `date`>=date_sub('${hivevar:day}',1) and `date`<='${hivevar:day}' ;

