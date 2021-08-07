-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.rpt_pangge_pay_daily_temp0 ;
create table if not exists temp.rpt_pangge_pay_daily_temp0 as 
select date_add(start_date, pos) as stati_date 
from ( select '1' as uid ,date_sub('${hivevar:day}',16) as start_date ,'${hivevar:day}' as end_date ) tmp lateral view posexplode( split( space( datediff( end_date, start_date ) ), '' ) ) t as pos, val;

drop table if exists temp.rpt_pangge_pay_daily_temp1 ;
create table if not exists temp.rpt_pangge_pay_daily_temp1 as select t1.city_market_id ,t1.city_market_name ,t1.shop_id ,t1.shop_name ,t1.coo_shop_type ,t1.period ,t1.pay_item_num ,t1.allot_item_num ,t2.receive_item_num ,t3.ship_item_num ,t4.return_item_num ,t4.return_success_item_num ,t1.`date` from (select * from rpt.rpt_pangge_pay_allot_daily where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}') t1 left join (select * from rpt.rpt_pangge_pay_receive_daily where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}') t2 on t1.`date`=t2.`date` and t1.city_market_id=t2.city_market_id and t1.coo_shop_type=t2.coo_shop_type and t1.period=t2.period and t1.shop_id=t2.shop_id left join (select * from rpt.rpt_pangge_pay_ship_daily where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}') t3 on t1.`date`=t3.`date` and t1.city_market_id=t3.city_market_id and t1.coo_shop_type=t3.coo_shop_type and t1.period=t3.period and t1.shop_id=t3.shop_id left join (select * from rpt.rpt_pangge_return_success_daily where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}') t4 on t1.`date`=t4.`date` and t1.city_market_id=t4.city_market_id and t1.coo_shop_type=t4.coo_shop_type and t1.period=t4.period and t1.shop_id=t4.shop_id ;

drop table if exists temp.rpt_pangge_pay_daily_temp2 ;
create table if not exists temp.rpt_pangge_pay_daily_temp2 as select t2.* ,'${hivevar:day}' as update_date from temp.rpt_pangge_pay_daily_temp0 t1 inner join temp.rpt_pangge_pay_daily_temp1 t2 on t2.`date`=t1.stati_date ;

insert overwrite table rpt.rpt_pangge_pay_daily partition(`date`) select city_market_id ,city_market_name ,shop_id ,shop_name ,coo_shop_type ,period ,pay_item_num ,allot_item_num ,receive_item_num ,ship_item_num ,return_item_num ,return_success_item_num ,update_date ,`date` from temp.rpt_pangge_pay_daily_temp2 where `date`>=date_sub('${hivevar:day}',16) and `date`<date_add('${hivevar:day}',1) ;

