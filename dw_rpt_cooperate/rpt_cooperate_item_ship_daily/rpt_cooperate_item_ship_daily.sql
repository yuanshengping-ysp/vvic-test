SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp0 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp0 as
 select date_add(start_date, pos) as stati_date 
from ( select '1' as uid ,date_sub('${hivevar:day}',8) as start_date ,'${hivevar:day}' as end_date ) tmp lateral view posexplode( split( space( datediff( end_date, start_date ) ), '' ) ) t as pos, val;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp1 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp1 as select explode(split('1,2,7',',')) as periods ;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp2 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp2 as select `date` ,shop_id ,item_id ,order_details_id ,item_sku_id ,item_sku_price ,count(1) as pay_item_num from dw.fct_cooperate_pay_label where `date`>=date_sub('${hivevar:day}',8) and `date`<='${hivevar:day}' group by `date` ,shop_id ,item_id ,order_details_id ,item_sku_id ,item_sku_price ;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp3 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp3 as select t1.* ,t2.stati_date from temp.rpt_cooperate_item_ship_daily_temp2 t1 inner join temp.rpt_cooperate_item_ship_daily_temp0 t2 where t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp4 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp4 as select t1.* ,t2.periods from temp.rpt_cooperate_item_ship_daily_temp3 t1 ,temp.rpt_cooperate_item_ship_daily_temp1 t2 ;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp5 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp5 as select t1.`date` ,t1.shop_id ,t1.item_id ,t1.order_details_id ,t1.item_sku_id ,t1.item_sku_price ,(unix_timestamp(t1.ship_time)-unix_timestamp(t1.pay_time))*1.0/86400 as period ,count(1) as ship_item_num from dw.fct_cooperate_ship_label t1 where t1.`date`>=date_sub('${hivevar:day}',8) group by t1.`date` ,t1.shop_id ,t1.item_id ,t1.order_details_id ,t1.item_sku_id ,t1.item_sku_price ,(unix_timestamp(t1.ship_time)-unix_timestamp(t1.pay_time))*1.0/86400 ;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp6 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp6 as select t1.* ,cast(t2.periods as int) as periods from temp.rpt_cooperate_item_ship_daily_temp5 t1 ,temp.rpt_cooperate_item_ship_daily_temp1 t2 where t1.period<=cast(t2.periods as int) ;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp7 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp7 as select t1.`date` ,t1.shop_id ,t1.item_id ,t1.order_details_id ,t1.item_sku_id ,t1.item_sku_price ,t1.pay_item_num ,t1.stati_date ,t1.periods ,t2.period ,t2.ship_item_num from temp.rpt_cooperate_item_ship_daily_temp4 t1 left join temp.rpt_cooperate_item_ship_daily_temp6 t2 on cast(t1.order_details_id as string)=cast(t2.order_details_id as string) and t1.periods=t2.periods ;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp8 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp8 as select '${hivevar:day}' as update_date ,`date` ,shop_id ,item_id ,order_details_id ,item_sku_id ,item_sku_price ,periods as period ,sum(pay_item_num) as pay_item_num ,sum(ship_item_num) as ship_item_num  from temp.rpt_cooperate_item_ship_daily_temp7 group by `date` ,shop_id ,item_id ,order_details_id ,item_sku_id ,item_sku_price ,periods ;

drop table if exists temp.rpt_cooperate_item_ship_daily_temp9 ;
create table if not exists temp.rpt_cooperate_item_ship_daily_temp9 as select t1.update_date ,t1.`date` ,t1.shop_id ,t2.name as shop_name ,t2.bid ,t3.name as market_name ,t1.item_id ,t1.order_details_id ,t1.item_sku_id ,t1.item_sku_price ,t1.period ,t1.pay_item_num ,t1.ship_item_num  from temp.rpt_cooperate_item_ship_daily_temp8 t1 left join ods.t_shop t2 on t1.shop_id=t2.id left join (select distinct bid,name from ods.t_market) t3 on t2.bid=t3.bid;

insert overwrite table rpt.rpt_cooperate_item_ship_daily partition(`date`) select shop_id ,shop_name ,bid ,market_name ,order_details_id ,item_id ,item_sku_id ,item_sku_price ,period ,pay_item_num ,ship_item_num ,update_date ,`date` from temp.rpt_cooperate_item_ship_daily_temp9 where `date`>=date_sub('${hivevar:day}',8) and `date`<='${hivevar:day}' ;

