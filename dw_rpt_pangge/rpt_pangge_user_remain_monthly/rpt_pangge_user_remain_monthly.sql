-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;



drop table if exists temp.rpt_pangge_user_remain_monthly_temp1 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp1 as select '${hivevar:day}' as month ;

select count(1) as num from temp.rpt_pangge_user_remain_monthly_temp1 ;
drop table if exists temp.rpt_pangge_user_remain_monthly_temp2 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp2 as select explode(split('0,1,2,3,4,5,6,7,8,9,10,11,12',',')) as period ;

drop table if exists temp.rpt_pangge_user_remain_monthly_temp3 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp3 as select t1.month ,t2.period ,(t1.month+t2.period) as `date` from temp.rpt_pangge_user_remain_monthly_temp1 t1 ,temp.rpt_pangge_user_remain_monthly_temp2 t2 ;

drop table if exists temp.rpt_pangge_user_remain_monthly_temp4 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp4 as select month ,period ,`date` ,(case when pmod(`date`,100)>12 then `date`+100-12 else `date` end) as period_month from temp.rpt_pangge_user_remain_monthly_temp3 where `date`<=from_unixtime(unix_timestamp(),'yyyyMM') ;

drop table if exists temp.rpt_pangge_user_remain_monthly_temp5 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp5 as select user_id ,(year(`date`)*100+month(`date`)) as month ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,count(distinct order_no) as pay_order_num from dw.fct_pangge_pay_order group by user_id ,(year(`date`)*100+month(`date`)) ;

drop table if exists temp.rpt_pangge_user_remain_monthly_temp6 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp6 as select * ,lag(month,1) over (partition by user_id order by month) as last_month from temp.rpt_pangge_user_remain_monthly_temp5 ;

drop table if exists temp.rpt_pangge_user_remain_monthly_temp7 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp7 as select * ,(case when last_month is null then 'new' when ((int(month/100)-int(last_month/100))*12+pmod(month,100)-pmod(last_month,100))=1 then 'remain' when ((int(month/100)-int(last_month/100))*12+pmod(month,100)-pmod(last_month,100))>1 then 'recall' else null end) as user_type from temp.rpt_pangge_user_remain_monthly_temp6 ;

drop table if exists temp.rpt_pangge_user_remain_monthly_temp8 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp8 as select t1.* ,t2.period ,t2.period_month from temp.rpt_pangge_user_remain_monthly_temp7 t1 inner join temp.rpt_pangge_user_remain_monthly_temp4 t2 on t1.month=t2.month ;

drop table if exists temp.rpt_pangge_user_remain_monthly_temp9 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp9 as select t1.* ,t2.user_id as remain_user_id ,t2.pay_order_num as remain_pay_order_num ,t2.pay_item_num as remain_pay_item_num ,t2.pay_order_money as remain_pay_order_money from temp.rpt_pangge_user_remain_monthly_temp8 t1 left join temp.rpt_pangge_user_remain_monthly_temp7 t2 on t1.period_month=t2.month and t1.user_id=t2.user_id ;

drop table if exists temp.rpt_pangge_user_remain_monthly_temp10 ;
create table if not exists temp.rpt_pangge_user_remain_monthly_temp10 as select month ,0 as `type` ,period ,count(distinct user_id) as pay_uv ,sum(pay_order_num) as pay_order_num ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,count(distinct remain_user_id) as remain_pay_uv ,sum(remain_pay_order_num) as remain_pay_order_num ,sum(remain_pay_item_num) as remain_pay_item_num ,sum(remain_pay_order_money) as remain_pay_order_money from temp.rpt_pangge_user_remain_monthly_temp9 group by month ,period union all select month ,1 as `type` ,period ,count(distinct user_id) as pay_uv ,sum(pay_order_num) as pay_order_num ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,count(distinct remain_user_id) as remain_pay_uv ,sum(remain_pay_order_num) as remain_pay_order_num ,sum(remain_pay_item_num) as remain_pay_item_num ,sum(remain_pay_order_money) as remain_pay_order_money  from temp.rpt_pangge_user_remain_monthly_temp9 where user_type='new' group by month ,period ;

insert overwrite table rpt.rpt_pangge_user_remain_monthly partition(month='${hivevar:day}') select `type` ,period ,pay_uv ,pay_order_num ,pay_item_num ,pay_order_money ,remain_pay_uv ,remain_pay_order_num ,remain_pay_item_num ,remain_pay_order_money from temp.rpt_pangge_user_remain_monthly_temp10 ;

