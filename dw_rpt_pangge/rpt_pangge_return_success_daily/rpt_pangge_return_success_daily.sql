-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.rpt_pangge_return_success_daily_temp0 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp0 as 
select date_add(start_date, pos) as stati_date 
from ( select '1' as uid ,date_sub('${hivevar:day}',16) as start_date ,'${hivevar:day}' as end_date ) tmp lateral view posexplode( split( space( datediff( end_date, start_date ) ), '' ) ) t as pos, val;

drop table if exists temp.rpt_pangge_return_success_daily_temp1 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp1 as select * ,lead(create_time,1) over (partition by return_label_no order by create_time) as success_time from dw.fct_pangge_return_label where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}' and return_type=0 ;

drop table if exists temp.rpt_pangge_return_success_daily_temp2 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp2 as select t1.* from temp.rpt_pangge_return_success_daily_temp1 t1 inner join temp.rpt_pangge_return_success_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_return_success_daily_temp3 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp3 as select t1.* ,(case when t2.shop_id is null then 2 else 1 end) coo_shop_type from temp.rpt_pangge_return_success_daily_temp2 t1 left join (select `date` ,shop_id from dw.fct_cooperate_shop where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}'and coo_shop_num=1) t2 on cast(t1.shop_id as string)=t2.shop_id and t1.`date`=t2.`date` ;

drop table if exists temp.rpt_pangge_return_success_daily_temp4 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp4 as select * from temp.rpt_pangge_return_success_daily_temp3 where `type`=0 ;

drop table if exists temp.rpt_pangge_return_success_daily_temp5 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp5 as select * ,(unix_timestamp(success_time)-unix_timestamp(create_time))*1.0/3600 as periods from temp.rpt_pangge_return_success_daily_temp4 ;

drop table if exists temp.rpt_pangge_return_success_daily_temp6 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp6 as select cast(t1.period as int) as period from (select explode(split('12,24,48,72,168,360',',')) as period) t1 ;

drop table if exists temp.rpt_pangge_return_success_daily_temp7 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp7 as select t1.* ,t2.period from temp.rpt_pangge_return_success_daily_temp5 t1 ,temp.rpt_pangge_return_success_daily_temp6 t2 ;

drop table if exists temp.rpt_pangge_return_success_daily_temp8 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp8 as select `date` ,0 as city_market_id ,0 as shop_id ,0 as coo_shop_type ,period ,count(1) as return_item_num ,count(case when success_time is not null and periods<=period then 1 else null end) as return_success_item_num from temp.rpt_pangge_return_success_daily_temp7 group by `date` ,period union all select `date` ,city_market_id ,0 as shop_id ,0 as coo_shop_type ,period ,count(1) as return_item_num ,count(case when success_time is not null and periods<=period then 1 else null end) as return_success_item_num from temp.rpt_pangge_return_success_daily_temp7 group by `date` ,city_market_id ,period union all select `date` ,0 as city_market_id ,0 as shop_id ,coo_shop_type ,period ,count(1) as return_item_num ,count(case when success_time is not null and periods<=period then 1 else null end) as return_success_item_num from temp.rpt_pangge_return_success_daily_temp7 group by `date` ,coo_shop_type ,period union all select `date` ,city_market_id ,0 as shop_id ,coo_shop_type ,period ,count(1) as return_item_num ,count(case when success_time is not null and periods<=period then 1 else null end) as return_success_item_num from temp.rpt_pangge_return_success_daily_temp7 group by `date` ,city_market_id ,coo_shop_type ,period union all select `date` ,city_market_id ,shop_id ,coo_shop_type ,period ,count(1) as return_item_num ,count(case when success_time is not null and periods<=period then 1 else null end) as return_success_item_num from temp.rpt_pangge_return_success_daily_temp7 group by `date` ,city_market_id ,shop_id ,coo_shop_type ,period ;

drop table if exists temp.rpt_pangge_return_success_daily_temp9 ;
create table if not exists temp.rpt_pangge_return_success_daily_temp9 as select t1.* ,(case when t2.name is null then '总体' else t2.name end) as city_market_name ,(case when t1.shop_id=0 then '总体' else t3.name end) as shop_name ,'${hivevar:day}' as update_date from temp.rpt_pangge_return_success_daily_temp8 t1 left join ods.t_city_market t2 on t1.city_market_id=t2.id left join ods.t_shop t3 on t1.shop_id=t3.id ;

insert overwrite table rpt.rpt_pangge_return_success_daily partition(`date`) select city_market_id ,city_market_name ,shop_id ,shop_name ,coo_shop_type ,period ,return_item_num ,return_success_item_num ,update_date ,`date` from temp.rpt_pangge_return_success_daily_temp9 where `date`>=date_sub('${hivevar:day}',16) and `date`<date_add('${hivevar:day}',1) ;

