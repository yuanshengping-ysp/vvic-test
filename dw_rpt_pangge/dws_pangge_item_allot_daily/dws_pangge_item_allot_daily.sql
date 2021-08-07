-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.dws_pangge_item_allot_daily_temp0 ;
create table if not exists temp.dws_pangge_item_allot_daily_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.dws_pangge_item_allot_daily_temp1 ;
create table if not exists temp.dws_pangge_item_allot_daily_temp1 as select 'allot' as `indicator` ,`product_label_no` ,`date` from dwd.dwd_pangge_allot_label where `date`>='${hivevar:day}' and `date`<='${hivevar:day}' union all select 'enallot' as `indicator` ,`product_label_no` ,`date` from dwd.dwd_pangge_enallot_label where `date`>='${hivevar:day}' and `date`<='${hivevar:day}' union all select 'print' as `indicator` ,`product_label_no` ,`date` from dwd.dwd_pangge_print_label where `date`>='${hivevar:day}' and `date`<='${hivevar:day}' ;

drop table if exists temp.dws_pangge_item_allot_daily_temp2 ;
create table if not exists temp.dws_pangge_item_allot_daily_temp2 as select t3.* ,t4.item_id ,t4.`shop_id` ,t4.`city_market_id` ,t5.`bid` as `market_id` ,(case when t6.coo_shop_num is null then 0 else 1 end) as `is_coo_shop` from (select t1.* from temp.dws_pangge_item_allot_daily_temp1 t1 inner join temp.dws_pangge_item_allot_daily_temp0 t2 on t1.`date`=t2.`stati_date`) t3 left join dw.dim_wms_product_label t4 on t3.`product_label_no`=t4.`product_label_no` left join ods.t_shop t5 on t4.`shop_id`=t5.`id` left join (select * from dw.fct_cooperate_shop where `date`>='${hivevar:day}' and `date`<='${hivevar:day}' and `coo_shop_num`=1) t6 on t4.`shop_id`=t6.`shop_id` and t3.`date`=t6.`date` ;

drop table if exists temp.dws_pangge_item_allot_daily_temp3 ;
create table if not exists temp.dws_pangge_item_allot_daily_temp3 as select `date`,`city_market_id`,`market_id`,`is_coo_shop`,`shop_id`,`item_id`,count(case when `indicator`='allot' then 1 else null end) as `allot_item_num`,count(case when `indicator`='print' then 1 else null end) as `print_item_num`,count(case when `indicator`='enallot' then 1 else null end) as `enallot_item_num`  from temp.dws_pangge_item_allot_daily_temp2 group by `date`,`city_market_id`,`market_id`,`is_coo_shop`,`shop_id`,`item_id`  ;

insert overwrite table dws.dws_pangge_item_allot_daily partition(`date`='${hivevar:day}') select `city_market_id` ,`market_id` ,`is_coo_shop` ,`shop_id` ,`item_id` ,`allot_item_num` ,`print_item_num` ,`enallot_item_num` from temp.dws_pangge_item_allot_daily_temp3 where `date`='${hivevar:day}' ;

