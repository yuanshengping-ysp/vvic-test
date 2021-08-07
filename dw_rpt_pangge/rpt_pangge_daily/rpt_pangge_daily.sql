-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.rpt_pangge_daily_temp0 ;
create table if not exists temp.rpt_pangge_daily_temp0 as 
select date_add(start_date, pos) as stati_date 
from ( select '1' as uid ,date_sub('${hivevar:day}',16) as start_date ,'${hivevar:day}' as end_date ) tmp lateral view posexplode( split( space( datediff( end_date, start_date ) ), '' ) ) t as pos, val;

drop table if exists temp.rpt_pangge_daily_temp1 ;
create table if not exists temp.rpt_pangge_daily_temp1 as select t1.* from (select * from rpt.rpt_pangge_pay_order_daily where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}') t1 inner join temp.rpt_pangge_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_daily_temp2 ;
create table if not exists temp.rpt_pangge_daily_temp2 as select t1.* from (select * from rpt.rpt_pangge_ship_daily where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}') t1 inner join temp.rpt_pangge_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_daily_temp3 ;
create table if not exists temp.rpt_pangge_daily_temp3 as select t1.* from (select `date` ,city_market_id ,city_market_name ,sum(case when period=-1 then ship_item_num else 0 end) as pay_ship_item_num ,sum(case when period=-1 then ship_order_num else 0 end) as pay_ship_order_num ,sum(case when period=-1 then ship_order_money else 0 end) as pay_ship_order_money ,sum(case when period=-1 then ship_item_money else 0 end) as pay_ship_item_money ,sum(case when period=1 then pay_item_num else 0 end) as pay_item_num_1d ,sum(case when period=1 then ship_item_num else 0 end) as pay_ship_item_num_1d ,sum(case when period=2 then pay_item_num else 0 end) as pay_item_num_2d ,sum(case when period=2 then ship_item_num else 0 end) as pay_ship_item_num_2d ,sum(case when period=7 then pay_item_num else 0 end) as pay_item_num_7d ,sum(case when period=7 then ship_item_num else 0 end) as pay_ship_item_num_7d from rpt.rpt_pangge_item_ship_daily where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}' group by `date` ,city_market_id ,city_market_name ) t1 inner join temp.rpt_pangge_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_daily_temp4 ;
create table if not exists temp.rpt_pangge_daily_temp4 as select t1.* from (select `date` ,city_market_id ,city_market_name ,sum(case when period=-1 then return_item_num else 0 end) as return_item_num ,sum(case when period=-1 then return_success_item_num else 0 end) as return_success_item_num ,sum(case when period=7 then return_item_num else 0 end) as return_item_num_7d ,sum(case when period=7 then return_success_item_num else 0 end) as return_success_item_num_7d ,sum(case when period=15 then return_item_num else 0 end) as return_item_num_15d ,sum(case when period=15 then return_success_item_num else 0 end) as return_success_item_num_15d from rpt.rpt_pangge_return_daily where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}' group by `date` ,city_market_id ,city_market_name ) t1 inner join temp.rpt_pangge_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_daily_temp5 ;
create table if not exists temp.rpt_pangge_daily_temp5 as select distinct t1.`date` ,t1.city_market_id ,t1.city_market_name from (select distinct `date` ,city_market_id ,city_market_name from temp.rpt_pangge_daily_temp1 union all select distinct `date` ,city_market_id ,city_market_name from temp.rpt_pangge_daily_temp2 union all select distinct `date` ,city_market_id ,city_market_name from temp.rpt_pangge_daily_temp3 union all select distinct `date` ,city_market_id ,city_market_name from temp.rpt_pangge_daily_temp4 ) t1 ;

drop table if exists temp.rpt_pangge_daily_temp6 ;
create table if not exists temp.rpt_pangge_daily_temp6 as select t1.`date` ,t1.city_market_id ,t1.city_market_name ,t2.pay_uv ,t2.pay_order_num ,t2.pay_item_num ,t2.coo_pay_item_num ,t2.pay_order_money ,t2.pay_item_money ,t2.new_pay_uv ,t2.new_pay_order_num ,t2.new_pay_item_num ,t2.new_pay_order_money ,t2.remain_pay_uv ,t2.remain_pay_order_num ,t2.remain_pay_item_num ,t2.remain_pay_order_money ,t2.recall_pay_uv ,t2.recall_pay_order_num ,t2.recall_pay_item_num ,t2.recall_pay_order_money ,t2.pay_shop_num ,t2.coo_pay_shop_num ,t2.pay_uv_sd ,t2.last_pay_uv_sd ,t2.last_pay_uv ,t3.ship_order_money ,t3.ship_item_money ,t3.ship_item_num ,t3.ship_order_num ,t4.pay_ship_order_money ,t4.pay_ship_item_money ,t4.pay_ship_item_num ,t4.pay_ship_order_num ,t4.pay_ship_item_num_1d ,t4.pay_ship_item_num_2d ,t4.pay_ship_item_num_7d ,t5.return_item_num ,t5.return_success_item_num ,t5.return_item_num_7d ,t5.return_success_item_num_7d ,t5.return_success_item_num_15d from temp.rpt_pangge_daily_temp5 t1 left join temp.rpt_pangge_daily_temp1 t2 on t1.`date`=t2.`date` and t1.city_market_id=t2.city_market_id left join temp.rpt_pangge_daily_temp2 t3 on t1.`date`=t3.`date` and t1.city_market_id=t3.city_market_id left join temp.rpt_pangge_daily_temp3 t4 on t1.`date`=t4.`date` and t1.city_market_id=t4.city_market_id left join temp.rpt_pangge_daily_temp4 t5 on t1.`date`=t5.`date` and t1.city_market_id=t5.city_market_id ;


insert overwrite table rpt.rpt_pangge_daily partition(`date`) select city_market_id ,city_market_name ,pay_uv ,pay_order_num ,pay_item_num ,coo_pay_item_num ,pay_order_money ,pay_item_money ,new_pay_uv ,new_pay_order_num ,new_pay_item_num ,new_pay_order_money ,remain_pay_uv ,remain_pay_order_num ,remain_pay_item_num ,remain_pay_order_money ,recall_pay_uv ,recall_pay_order_num ,recall_pay_item_num ,recall_pay_order_money ,pay_shop_num ,coo_pay_shop_num ,pay_uv_sd ,last_pay_uv_sd ,last_pay_uv ,ship_order_money ,ship_item_money ,ship_item_num ,ship_order_num ,pay_ship_order_money ,pay_ship_item_money ,pay_ship_item_num ,pay_ship_order_num ,pay_ship_item_num_1d ,pay_ship_item_num_2d ,pay_ship_item_num_7d ,return_item_num ,return_success_item_num ,return_success_item_num_7d ,return_success_item_num_15d ,`date` from temp.rpt_pangge_daily_temp6 where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}' ;

