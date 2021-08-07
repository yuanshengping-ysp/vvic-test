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

drop table if exists temp.rpt_pangge_return_daily_temp0 ;
create table if not exists temp.rpt_pangge_return_daily_temp0 as 
select date_add(start_date, pos) as stati_date 
from ( select '1' as uid ,date_sub('${hivevar:day}',16) as start_date ,'${hivevar:day}' as end_date ) tmp lateral view posexplode( split( space( datediff( end_date, start_date ) ), '' ) ) t as pos, val;

drop table if exists temp.rpt_pangge_return_daily_temp1 ;
create table if not exists temp.rpt_pangge_return_daily_temp1 as select explode(split('0,7',',')) as periods ;

drop table if exists temp.rpt_pangge_return_daily_temp2 ;
create table if not exists temp.rpt_pangge_return_daily_temp2 as select * ,lead(create_time,1) over (partition by return_label_no order by create_time) as success_time from dw.fct_pangge_return_label where `date`>=date_sub('${hivevar:day}',16) and return_type=0 ;

drop table if exists temp.rpt_pangge_return_daily_temp3 ;
create table if not exists temp.rpt_pangge_return_daily_temp3 as select t1.* ,(unix_timestamp(t1.success_time)-unix_timestamp(t1.create_time))*1.0/86400 as period from temp.rpt_pangge_return_daily_temp2 t1 inner join temp.rpt_pangge_return_daily_temp0 t2 on t1.`date`=t2.stati_date where t1.`type`=0 ;

drop table if exists temp.rpt_pangge_return_daily_temp4 ;
create table if not exists temp.rpt_pangge_return_daily_temp4 as select `date` ,city_market_id ,-1 as period ,sum(return_item_num) as return_item_num ,sum(case when substring(success_time,1,10)=substring(create_time,1,10) then return_item_num else 0 end) as return_success_item_num from temp.rpt_pangge_return_daily_temp3 group by `date` ,city_market_id union all select `date` ,0 as city_market_id ,-1 as period ,sum(return_item_num) as return_item_num ,sum(case when substring(success_time,1,10)=substring(create_time,1,10) then return_item_num else 0 end) as return_success_item_num from temp.rpt_pangge_return_daily_temp3 group by `date` union all select `date` ,city_market_id ,7 as period ,sum(return_item_num) as return_item_num ,sum(case when period<=7 then return_item_num else 0 end) as return_success_item_num from temp.rpt_pangge_return_daily_temp3 group by `date` ,city_market_id union all select `date` ,0 as city_market_id ,7 as period ,sum(return_item_num) as return_item_num ,sum(case when period<=7 then return_item_num else 0 end) as return_success_item_num from temp.rpt_pangge_return_daily_temp3 group by `date` union all select `date` ,city_market_id ,15 as period ,sum(return_item_num) as return_item_num ,sum(case when period<=15 then return_item_num else 0 end) as return_success_item_num from temp.rpt_pangge_return_daily_temp3 group by `date` ,city_market_id union all select `date` ,0 as city_market_id ,15 as period ,sum(return_item_num) as return_item_num ,sum(case when period<=15 then return_item_num else 0 end) as return_success_item_num from temp.rpt_pangge_return_daily_temp3 group by `date` ;

drop table if exists temp.rpt_pangge_return_daily_temp5 ;
create table if not exists temp.rpt_pangge_return_daily_temp5 as select t1.* ,(case when t2.name is null then '总体' else t2.name end) as city_market_name ,'${hivevar:day}' as update_date from temp.rpt_pangge_return_daily_temp4 t1 left join ods.t_city_market t2 on t1.city_market_id=t2.id ;


insert overwrite table rpt.rpt_pangge_return_daily partition(`date`) select city_market_id ,city_market_name ,period ,return_item_num ,return_success_item_num ,update_date ,`date` from temp.rpt_pangge_return_daily_temp5 where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}' ;

