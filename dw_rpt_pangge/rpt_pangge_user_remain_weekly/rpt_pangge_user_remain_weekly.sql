-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;


drop table if exists temp.rpt_pangge_user_remain_weekly_temp0 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp0 as 
select date_add(start_date, pos) as stati_date 
from ( select '1' as uid ,'2020-01-01' as start_date ,'${hivevar:day}' as end_date ) tmp lateral view posexplode( split( space( datediff( end_date, start_date ) ), '' ) ) t as pos, val;

drop table if exists temp.rpt_pangge_user_remain_weekly_temp1 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp1 as select stati_date, (year(date_sub(next_day(stati_date,'MO'),4))*100+weekofyear(stati_date)) as weeknum from temp.rpt_pangge_user_remain_weekly_temp0 where datediff(next_sunday,stati_date)=7 ;

select count(1) as num from temp.rpt_pangge_user_remain_weekly_temp1 ;
drop table if exists temp.rpt_pangge_user_remain_weekly_temp2 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp2 as select explode(split('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24',',')) as period ;

drop table if exists temp.rpt_pangge_user_remain_weekly_temp3 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp3 as select t1.stati_date ,t1.weeknum ,t2.period ,date_add(t1.stati_date, cast(t2.period as int)*7) as `date` from temp.rpt_pangge_user_remain_weekly_temp1 t1 ,temp.rpt_pangge_user_remain_weekly_temp2 t2 ;

drop table if exists temp.rpt_pangge_user_remain_weekly_temp4 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp4 as select stati_date ,weeknum ,period ,`date` ,(year(date_sub(next_day(`date`,'MO'),4))*100+weekofyear(`date`)) as period_weeknum ,date_sub(stati_date,6) as begin_date from temp.rpt_pangge_user_remain_weekly_temp3 where `date`<date_add('${hivevar:day}',1) ;

drop table if exists temp.rpt_pangge_user_remain_weekly_temp5 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp5 as select user_id ,(year(date_sub(next_day(`date`,'MO'),4))*100+weekofyear(`date`)) as weeknum ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,count(distinct order_no) as pay_order_num from dw.fct_pangge_pay_order group by user_id ,(year(date_sub(next_day(`date`,'MO'),4))*100+weekofyear(`date`)) ;

drop table if exists temp.rpt_pangge_user_remain_weekly_temp6 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp6 as select * ,lag(weeknum,1) over (partition by user_id order by weeknum) as last_weeknum from temp.rpt_pangge_user_remain_weekly_temp5 ;

drop table if exists temp.rpt_pangge_user_remain_weekly_temp7 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp7 as select * ,(case when last_weeknum is null then 'new' when ((int(weeknum/100)-int(last_weeknum/100))*52+pmod(weeknum,100)-pmod(last_weeknum,100))=1 then 'remain' when ((int(weeknum/100)-int(last_weeknum/100))*52+pmod(weeknum,100)-pmod(last_weeknum,100))>1 then 'recall' else null end) as user_type from temp.rpt_pangge_user_remain_weekly_temp6 ;

drop table if exists temp.rpt_pangge_user_remain_weekly_temp8 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp8 as select t1.* ,t2.stati_date as `date` ,t2.period ,t2.period_weeknum ,t2.begin_date from temp.rpt_pangge_user_remain_weekly_temp7 t1 inner join temp.rpt_pangge_user_remain_weekly_temp4 t2 on t1.weeknum=t2.weeknum ;

drop table if exists temp.rpt_pangge_user_remain_weekly_temp9 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp9 as select t1.* ,t2.user_id as remain_user_id ,t2.pay_item_num as remain_pay_item_num ,t2.pay_order_money as remain_pay_order_money ,t2.pay_order_num  as remain_pay_order_num from temp.rpt_pangge_user_remain_weekly_temp8 t1 left join temp.rpt_pangge_user_remain_weekly_temp7 t2 on t1.period_weeknum=t2.weeknum and t1.user_id=t2.user_id ;

drop table if exists temp.rpt_pangge_user_remain_weekly_temp10 ;
create table if not exists temp.rpt_pangge_user_remain_weekly_temp10 as select weeknum ,`date` ,begin_date ,0 as `type` ,period ,count(distinct user_id) as pay_uv ,sum(pay_order_num) as pay_order_num ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,count(distinct remain_user_id) as remain_pay_uv ,sum(remain_pay_order_num) as remain_pay_order_num ,sum(remain_pay_item_num) as remain_pay_item_num ,sum(remain_pay_order_money) as remain_pay_order_money from temp.rpt_pangge_user_remain_weekly_temp9 group by weeknum ,`date` ,begin_date ,period union all select weeknum ,`date` ,begin_date ,1 as `type` ,period ,count(distinct user_id) as pay_uv ,sum(pay_order_num) as pay_order_num ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,count(distinct remain_user_id) as remain_pay_uv ,sum(remain_pay_order_num) as remain_pay_order_num ,sum(remain_pay_item_num) as remain_pay_item_num ,sum(remain_pay_order_money) as remain_pay_order_money from temp.rpt_pangge_user_remain_weekly_temp9 where user_type='new' group by weeknum ,`date` ,begin_date ,period ;


insert overwrite table rpt.rpt_pangge_user_remain_weekly partition(`date`) select weeknum ,begin_date ,`type` ,period ,pay_uv ,pay_order_num ,pay_item_num ,pay_order_money ,remain_pay_uv ,remain_pay_order_num ,remain_pay_item_num ,remain_pay_order_money ,`date` from temp.rpt_pangge_user_remain_weekly_temp10 where `date`>='2020-01-01' and `date`<date_sub('${hivevar:day}',16) ;

insert overwrite table rpt.rpt_pangge_user_remain_weekly partition(`date`) select weeknum ,begin_date ,`type` ,period ,pay_uv ,pay_order_num ,pay_item_num ,pay_order_money ,remain_pay_uv ,remain_pay_order_num ,remain_pay_item_num ,remain_pay_order_money ,`date` from temp.rpt_pangge_user_remain_weekly_temp10 where `date`>=date_sub('${hivevar:day}',16) and `date`<'${hivevar:day}' ;

