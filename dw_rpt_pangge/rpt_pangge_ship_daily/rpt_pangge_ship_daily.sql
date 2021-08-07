-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;
SET hive.execution.engine=tez;
SET hive.tez.container.size=1024;
SET hive.vectorized.execution.enabled=FALSE;


drop table if exists temp.rpt_pangge_ship_daily_temp0 ;
create table if not exists temp.rpt_pangge_ship_daily_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.rpt_pangge_ship_daily_temp1 ;
create table if not exists temp.rpt_pangge_ship_daily_temp1 as select `date` ,city_market_id ,order_no ,sum(ship_item_num) as ship_item_num ,sum(item_sku_price*ship_item_num) as ship_item_money from dw.fct_pangge_ship_item where `date`>='${hivevar:day}' and `date`<='${hivevar:day}' group by `date` ,city_market_id ,order_no ;

drop table if exists temp.rpt_pangge_ship_daily_temp2 ;
create table if not exists temp.rpt_pangge_ship_daily_temp2 as select t1.* from temp.rpt_pangge_ship_daily_temp1 t1 inner join temp.rpt_pangge_ship_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_ship_daily_temp3 ;
create table if not exists temp.rpt_pangge_ship_daily_temp3 as select t1.* ,t2.ship_order_money from temp.rpt_pangge_ship_daily_temp2 t1 left join (select order_no ,sum(case when process_type=1 then amount*(-1) else amount end) as ship_order_money from ods.vvic_user_trade_subject where trade_type in (1,2,3) and trade_name in ('搜款网下单','补差价','退差价') group by order_no) t2 on t1.order_no=t2.order_no ;

drop table if exists temp.rpt_pangge_ship_daily_temp4 ;
create table if not exists temp.rpt_pangge_ship_daily_temp4 as select `date` ,city_market_id ,count(distinct order_no) as ship_order_num ,sum(ship_order_money) as ship_order_money ,sum(ship_item_num) as ship_item_num ,sum(ship_item_money) as ship_item_money from temp.rpt_pangge_ship_daily_temp3 group by `date` ,city_market_id union all select `date` ,0 as city_market_id ,count(distinct order_no) as ship_order_num ,sum(ship_order_money) as ship_order_money ,sum(ship_item_num) as ship_item_num ,sum(ship_item_money) as ship_item_money from temp.rpt_pangge_ship_daily_temp3 group by `date` ;

drop table if exists temp.rpt_pangge_ship_daily_temp5 ;
create table if not exists temp.rpt_pangge_ship_daily_temp5 as select t1.* ,(case when t2.name is null then '总体' else t2.name end) as city_market_name from temp.rpt_pangge_ship_daily_temp4 t1 left join ods.t_city_market t2 on t1.city_market_id=t2.id ;

insert overwrite table rpt.rpt_pangge_ship_daily partition(`date`='${hivevar:day}') select city_market_id ,city_market_name ,ship_order_num ,ship_order_money ,ship_item_num ,ship_item_money from temp.rpt_pangge_ship_daily_temp5 ;

