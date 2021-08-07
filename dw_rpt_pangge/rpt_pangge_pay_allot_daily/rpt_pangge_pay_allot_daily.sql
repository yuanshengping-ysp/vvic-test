-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;


drop table if exists temp.rpt_pangge_pay_allot_daily_temp0 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp0 as 
select date_add(start_date, pos) as stati_date 
from ( select '1' as uid ,date_sub('${hivevar:day}',16) as start_date ,'${hivevar:day}' as end_date ) tmp lateral view posexplode( split( space( datediff( end_date, start_date ) ), '' ) ) t as pos, val;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp1 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp1 as select product_label_no ,city_market_id ,order_details_id ,item_sku_id ,item_sku_price ,order_no ,item_id ,user_id ,shop_id ,label_type ,create_time ,`date` from dw.dim_wms_product_label where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}' and order_type=0 ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp2 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp2 as select t1.* from temp.rpt_pangge_pay_allot_daily_temp1 t1 inner join temp.rpt_pangge_pay_allot_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp3 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp3 as select product_label_no ,min(create_time) as create_time from ods.wms__wms_product_label_log where create_time>=date_sub('${hivevar:day}',16) and event_type in ('011','012','013') and trigger_event regexp('分配次数：1|自行打标') group by product_label_no ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp4 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp4 as select t2.product_label_no ,t2.city_market_id ,t2.order_details_id ,t2.item_sku_id ,t2.item_sku_price ,t2.order_no ,t2.item_id ,t2.user_id ,t2.shop_id ,t2.label_type ,t1.create_time ,t2.`date` from temp.rpt_pangge_pay_allot_daily_temp3 t1 inner join temp.rpt_pangge_pay_allot_daily_temp2 t2 on t1.product_label_no=t2.product_label_no ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp5 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp5 as select product_label_no ,city_market_id ,order_details_id ,item_sku_id ,item_sku_price ,order_no ,item_id ,user_id ,shop_id ,label_type ,create_time ,`date` ,0 as `type` from temp.rpt_pangge_pay_allot_daily_temp2 union all select product_label_no ,city_market_id ,order_details_id ,item_sku_id ,item_sku_price ,order_no ,item_id ,user_id ,shop_id ,label_type ,create_time ,`date` ,1 as `type` from temp.rpt_pangge_pay_allot_daily_temp4 ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp6 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp6 as select t1.* ,(case when t2.shop_id is null then 2 else 1 end) coo_shop_type from temp.rpt_pangge_pay_allot_daily_temp5 t1 left join (select `date` ,shop_id from dw.fct_cooperate_shop where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}'and coo_shop_num=1) t2 on cast(t1.shop_id as string)=t2.shop_id and t1.`date`=t2.`date` ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp7 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp7 as select t1.* from (select * ,lead(create_time,1) over (partition by product_label_no order by create_time) as allot_time from temp.rpt_pangge_pay_allot_daily_temp6) t1 where t1.`type`=0 ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp8 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp8 as select * ,(unix_timestamp(allot_time)-unix_timestamp(create_time))*1.0/3600 as periods from temp.rpt_pangge_pay_allot_daily_temp7 ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp9 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp9 as select cast(t1.period as int) as period from (select explode(split('12,24,48,72,168,360',',')) as period) t1 ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp10 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp10 as select t1.* ,t2.period from temp.rpt_pangge_pay_allot_daily_temp8 t1 ,temp.rpt_pangge_pay_allot_daily_temp9 t2 ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp11 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp11 as select `date` ,0 as city_market_id ,0 as shop_id ,0 as coo_shop_type ,period ,count(case when label_type=0 then 1 else null end) as pay_item_num ,count(case when allot_time is not null and periods<=period then 1 else null end) as allot_item_num from temp.rpt_pangge_pay_allot_daily_temp10 group by `date` ,period union all select `date` ,city_market_id ,0 as shop_id ,0 as coo_shop_type ,period ,count(case when label_type=0 then 1 else null end) as pay_item_num ,count(case when allot_time is not null and periods<=period then 1 else null end) as allot_item_num from temp.rpt_pangge_pay_allot_daily_temp10 group by `date` ,city_market_id ,period union all select `date` ,0 as city_market_id ,0 as shop_id ,coo_shop_type ,period ,count(case when label_type in (0,1) then 1 else null end) as pay_item_num ,count(case when allot_time is not null and periods<=period then 1 else null end) as allot_item_num from temp.rpt_pangge_pay_allot_daily_temp10 group by `date` ,coo_shop_type ,period union all select `date` ,city_market_id ,0 as shop_id ,coo_shop_type ,period ,count(case when label_type in (0,1) then 1 else null end) as pay_item_num ,count(case when allot_time is not null and periods<=period then 1 else null end) as allot_item_num from temp.rpt_pangge_pay_allot_daily_temp10 group by `date` ,city_market_id ,coo_shop_type ,period union all select `date` ,city_market_id ,shop_id ,coo_shop_type ,period ,count(case when label_type in (0,1) then 1 else null end) as pay_item_num ,count(case when allot_time is not null and periods<=period then 1 else null end) as allot_item_num from temp.rpt_pangge_pay_allot_daily_temp10 group by `date` ,city_market_id ,shop_id ,coo_shop_type ,period ;

drop table if exists temp.rpt_pangge_pay_allot_daily_temp12 ;
create table if not exists temp.rpt_pangge_pay_allot_daily_temp12 as select t1.* ,(case when t2.name is null then '总体' else t2.name end) as city_market_name ,(case when t1.shop_id=0 then '总体' else t3.name end) as shop_name ,'${hivevar:day}' as update_date from temp.rpt_pangge_pay_allot_daily_temp11 t1 left join ods.t_city_market t2 on t1.city_market_id=t2.id left join ods.t_shop t3 on t1.shop_id=t3.id ;

insert overwrite table rpt.rpt_pangge_pay_allot_daily partition(`date`) select city_market_id ,city_market_name ,shop_id ,shop_name ,coo_shop_type ,period ,pay_item_num ,allot_item_num ,update_date ,`date` from temp.rpt_pangge_pay_allot_daily_temp12 where `date`>=date_sub('${hivevar:day}',16) and `date`<date_add('${hivevar:day}',1) ;

