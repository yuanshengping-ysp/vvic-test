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


drop table if exists temp.rpt_pangge_item_ship_daily_temp0 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp0 as 
select date_add(start_date, pos) as stati_date 
from ( select '1' as uid ,date_sub('${hivevar:day}',100) as start_date ,'${hivevar:day}' as end_date ) tmp lateral view posexplode( split( space( datediff( end_date, start_date ) ), '' ) ) t as pos, val;

drop table if exists temp.rpt_pangge_item_ship_daily_temp1 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp1 as select explode(split('1,2,7,100',',')) as periods ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp2 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp2 as select `date` ,city_market_id ,order_no ,order_details_id ,item_sku_price ,pay_item_num ,pay_order_money from dw.fct_pangge_pay_order_item where `date`>=date_sub('${hivevar:day}',100) and `date`<='${hivevar:day}' ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp3 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp3 as select t1.* from temp.rpt_pangge_item_ship_daily_temp2 t1 inner join temp.rpt_pangge_item_ship_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp4 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp4 as select t1.* ,cast(t2.periods as int) as periods from temp.rpt_pangge_item_ship_daily_temp3 t1 ,temp.rpt_pangge_item_ship_daily_temp1 t2 ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp5 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp5 as select `date` ,city_market_id ,order_no ,order_details_id ,(unix_timestamp(ship_time)-unix_timestamp(pay_time))*1.0/86400 as period ,ship_item_num from dw.fct_pangge_ship_item where `date`>=date_sub('${hivevar:day}',100) ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp6 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp6 as select t1.* ,cast(t2.periods as int) as periods from temp.rpt_pangge_item_ship_daily_temp5 t1 ,temp.rpt_pangge_item_ship_daily_temp1 t2 where t1.period<=cast(t2.periods as int) ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp7 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp7 as select t1.`date` ,t1.city_market_id ,t1.order_no ,t1.order_details_id ,t1.pay_item_num ,t1.periods ,t2.period ,t2.ship_item_num ,t2.order_no as ship_order_no ,(t1.item_sku_price*t2.ship_item_num) as ship_item_money ,t3.ship_order_money from temp.rpt_pangge_item_ship_daily_temp4 t1 left join temp.rpt_pangge_item_ship_daily_temp6 t2 on cast(t1.order_details_id as string)=cast(t2.order_details_id as string) and t1.periods=t2.periods left join (select order_no ,sum(case when process_type=1 then amount*(-1) else amount end) as ship_order_money from ods.vvic_user_trade_subject where create_time>=date_sub('${hivevar:day}',100) and trade_type in (1,2,3) and trade_name in ('搜款网下单','补差价','退差价') group by order_no) t3 on t2.order_no=t3.order_no ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp8 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp8 as select `date` ,city_market_id ,order_no ,ship_order_no ,periods ,ship_order_money ,sum(pay_item_num) as pay_item_num ,sum(ship_item_num) as ship_item_num ,sum(ship_item_money) as ship_item_money from temp.rpt_pangge_item_ship_daily_temp7 group by `date` ,city_market_id ,order_no ,ship_order_no ,periods ,ship_order_money ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp9 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp9 as select '${hivevar:day}' as update_date ,`date` ,city_market_id ,periods as period ,count(distinct order_no) as pay_order_num ,count(distinct ship_order_no) as ship_order_num ,sum(pay_item_num) as pay_item_num ,sum(ship_item_num) as ship_item_num ,sum(ship_item_money) as ship_item_money ,sum(ship_order_money) as ship_order_money from temp.rpt_pangge_item_ship_daily_temp8 group by `date` ,city_market_id ,periods union all select '${hivevar:day}' as update_date ,`date` ,0 as city_market_id ,periods as period ,count(distinct order_no) as pay_order_num ,count(distinct ship_order_no) as ship_order_num ,sum(pay_item_num) as pay_item_num ,sum(ship_item_num) as ship_item_num ,sum(ship_item_money) as ship_item_money ,sum(ship_order_money) as ship_order_money from temp.rpt_pangge_item_ship_daily_temp8 group by `date` ,periods ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp10 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp10 as select * ,-1 as period from temp.rpt_pangge_item_ship_daily_temp3 ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp11 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp11 as select `date` ,city_market_id ,order_no ,order_details_id ,-1 as period ,ship_item_num from dw.fct_pangge_ship_item where `date`>=date_sub('${hivevar:day}',100) and substring(ship_time,1,10)=substring(pay_time,1,10) ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp12 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp12 as select t1.`date` ,t1.city_market_id ,t1.order_no ,t1.order_details_id ,t1.pay_item_num ,t1.period ,t2.ship_item_num ,t2.order_no as ship_order_no ,(t1.item_sku_price*t2.ship_item_num) as ship_item_money ,(case when t2.order_no is not null then t1.pay_order_money else 0 end) as ship_order_money from temp.rpt_pangge_item_ship_daily_temp10 t1 left join temp.rpt_pangge_item_ship_daily_temp11 t2 on cast(t1.order_details_id as string)=cast(t2.order_details_id as string) and t1.period=t2.period ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp13 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp13 as select `date` ,city_market_id ,order_no ,ship_order_no ,period ,ship_order_money ,sum(pay_item_num) as pay_item_num ,sum(ship_item_num) as ship_item_num ,sum(ship_item_money) as ship_item_money from temp.rpt_pangge_item_ship_daily_temp12 group by `date` ,city_market_id ,order_no ,ship_order_no ,period ,ship_order_money ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp14 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp14 as select '${hivevar:day}' as update_date ,`date` ,city_market_id ,period ,count(distinct order_no) as pay_order_num ,count(distinct ship_order_no) as ship_order_num ,sum(pay_item_num) as pay_item_num ,sum(ship_item_num) as ship_item_num ,sum(ship_item_money) as ship_item_money ,sum(ship_order_money) as ship_order_money from temp.rpt_pangge_item_ship_daily_temp13 group by `date` ,city_market_id ,period union all select '${hivevar:day}' as update_date ,`date` ,0 as city_market_id ,period ,count(distinct order_no) as pay_order_num ,count(distinct ship_order_no) as ship_order_num ,sum(pay_item_num) as pay_item_num ,sum(ship_item_num) as ship_item_num ,sum(ship_item_money) as ship_item_money ,sum(ship_order_money) as ship_order_money from temp.rpt_pangge_item_ship_daily_temp13 group by `date` ,period ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp15 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp15 as select update_date ,`date` ,city_market_id ,period ,pay_order_num ,ship_order_num ,pay_item_num ,ship_item_num ,ship_item_money ,ship_order_money from temp.rpt_pangge_item_ship_daily_temp14 union all select update_date ,`date` ,city_market_id ,period ,pay_order_num ,ship_order_num ,pay_item_num ,ship_item_num ,ship_item_money ,ship_order_money from temp.rpt_pangge_item_ship_daily_temp9 ;

drop table if exists temp.rpt_pangge_item_ship_daily_temp16 ;
create table if not exists temp.rpt_pangge_item_ship_daily_temp16 as select t1.* ,(case when t2.name is null then '总体' else t2.name end) as city_market_name from temp.rpt_pangge_item_ship_daily_temp15 t1 left join ods.t_city_market t2 on t1.city_market_id=t2.id ;

insert overwrite table rpt.rpt_pangge_item_ship_daily partition(`date`) select city_market_id ,city_market_name ,period ,pay_order_num ,pay_item_num ,ship_order_num ,ship_item_num ,ship_item_money ,ship_order_money ,update_date ,`date` from temp.rpt_pangge_item_ship_daily_temp16 where `date`>=date_sub('${hivevar:day}',100) and `date`<='${hivevar:day}' ;

