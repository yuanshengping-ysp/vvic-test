-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table temp.rpt_pangge_user_remain_daily_temp0;
create table if not exists temp.rpt_pangge_user_remain_daily_temp0 as 
select date_add(start_date, pos) as stati_date 
from ( select '1' as uid ,date_sub('${hivevar:day}',100) as start_date ,'${hivevar:day}' as end_date ) tmp lateral view posexplode( split( space( datediff( end_date, start_date ) ), '' ) ) t as pos, val;

drop table if exists temp.rpt_pangge_user_remain_daily_temp1 ;
create table if not exists temp.rpt_pangge_user_remain_daily_temp1 as select explode(split('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,45,60,90',',')) as period ;

drop table if exists temp.rpt_pangge_user_remain_daily_temp2 ;
create table if not exists temp.rpt_pangge_user_remain_daily_temp2 as select t1.stati_date ,t2.period ,date_add(t1.stati_date, cast(t2.period as int)*1) as period_date from temp.rpt_pangge_user_remain_daily_temp0 t1 ,temp.rpt_pangge_user_remain_daily_temp1 t2 ;

drop table if exists temp.rpt_pangge_user_remain_daily_temp3 ;
create table if not exists temp.rpt_pangge_user_remain_daily_temp3 as select stati_date ,period ,period_date from temp.rpt_pangge_user_remain_daily_temp2 where period_date<date_add('${hivevar:day}',1) ;

drop table if exists temp.rpt_pangge_user_remain_daily_temp4 ;
create table if not exists temp.rpt_pangge_user_remain_daily_temp4 as select user_id ,`date` ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,count(distinct order_no) as pay_order_num from dw.fct_pangge_pay_order group by user_id ,`date` ;

drop table if exists temp.rpt_pangge_user_remain_daily_temp5 ;
create table if not exists temp.rpt_pangge_user_remain_daily_temp5 as select * ,lag(`date`,1) over (partition by user_id order by `date`) as last_date from temp.rpt_pangge_user_remain_daily_temp4 ;

drop table if exists temp.rpt_pangge_user_remain_daily_temp6 ;
create table if not exists temp.rpt_pangge_user_remain_daily_temp6 as select * ,(case when last_date is null then 'new' when datediff(`date`,last_date)=1 then 'remain' when datediff(`date`,last_date)>1 then 'recall' else null end) as user_type from temp.rpt_pangge_user_remain_daily_temp5 ;

drop table if exists temp.rpt_pangge_user_remain_daily_temp7 ;
create table if not exists temp.rpt_pangge_user_remain_daily_temp7 as select t1.* ,t2.period ,t2.period_date from temp.rpt_pangge_user_remain_daily_temp6 t1 inner join temp.rpt_pangge_user_remain_daily_temp3 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_user_remain_daily_temp8 ;
create table if not exists temp.rpt_pangge_user_remain_daily_temp8 as select t1.* ,t2.user_id as remain_user_id ,t2.pay_item_num as remain_pay_item_num ,t2.pay_order_money as remain_pay_order_money ,t2.pay_order_num  as remain_pay_order_num from temp.rpt_pangge_user_remain_daily_temp7 t1 left join temp.rpt_pangge_user_remain_daily_temp6 t2 on t1.period_date=t2.`date` and t1.user_id=t2.user_id ;

drop table if exists temp.rpt_pangge_user_remain_daily_temp9 ;
create table if not exists temp.rpt_pangge_user_remain_daily_temp9 as select `date` ,0 as `type` ,period ,count(distinct user_id) as pay_uv ,sum(pay_order_num) as pay_order_num ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,count(distinct remain_user_id) as remain_pay_uv ,sum(remain_pay_order_num) as remain_pay_order_num ,sum(remain_pay_item_num) as remain_pay_item_num ,sum(remain_pay_order_money) as remain_pay_order_money from temp.rpt_pangge_user_remain_daily_temp8 group by `date` ,period union all select `date` ,1 as `type` ,period ,count(distinct user_id) as pay_uv ,sum(pay_order_num) as pay_order_num ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,count(distinct remain_user_id) as remain_pay_uv ,sum(remain_pay_order_num) as remain_pay_order_num ,sum(remain_pay_item_num) as remain_pay_item_num ,sum(remain_pay_order_money) as remain_pay_order_money from temp.rpt_pangge_user_remain_daily_temp8 where user_type='new' group by `date` ,period ;

insert overwrite table rpt.rpt_pangge_user_remain_daily partition(`date`) select `type` ,period ,pay_uv ,pay_order_num ,pay_item_num ,pay_order_money ,remain_pay_uv ,remain_pay_order_num ,remain_pay_item_num ,remain_pay_order_money ,`date` from temp.rpt_pangge_user_remain_daily_temp9 ;
