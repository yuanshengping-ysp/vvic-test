-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.rpt_pangge_pay_order_daily_temp0 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp0 as select '${hivevar:day}' as stati_date ;


drop table if exists temp.rpt_pangge_pay_order_daily_temp1 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp1 as select t1.* ,t2.shop_id as coo_shop_id from (select * from dw.fct_pangge_pay_order_item ) t1 left join (select * from dw.fct_cooperate_shop ) t2 on t1.shop_id=t2.shop_id and t1.`date`=t2.`date` ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp2 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp2 as select t1.`date` ,t1.city_market_id ,t1.user_id ,sum(t1.pay_item_num) as pay_item_num ,sum(t1.pay_order_money) as pay_order_money ,count(distinct t1.order_no) as pay_order_num ,sum(t1.pay_item_money) as pay_item_money from (select `date` ,city_market_id ,user_id ,order_no ,pay_order_money ,pay_item_money ,sum(pay_item_num) as pay_item_num from temp.rpt_pangge_pay_order_daily_temp1 group by `date` ,city_market_id ,user_id ,order_no ,pay_order_money ,pay_item_money ) t1 group by t1.`date` ,t1.city_market_id ,t1.user_id ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp3 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp3 as select * ,lag(`date`,1) over (partition by city_market_id,user_id order by `date`) as last_date from temp.rpt_pangge_pay_order_daily_temp2 ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp4 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp4 as select * ,(case when last_date is null then 'new' when datediff(`date`,last_date)=1 then 'remain' when datediff(`date`,last_date)>1 then 'recall' else null end) as user_type from temp.rpt_pangge_pay_order_daily_temp3 ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp5 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp5 as select t1.* from temp.rpt_pangge_pay_order_daily_temp4 t1 inner join temp.rpt_pangge_pay_order_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp6 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp6 as select `date` ,city_market_id ,count(distinct user_id) as pay_uv ,sum(pay_order_num) as pay_order_num ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,sum(pay_item_money) as pay_item_money ,count(distinct (case when user_type='new' then user_id else null end)) as new_pay_uv ,sum(case when user_type='new' then pay_order_num else 0 end) as new_pay_order_num ,sum(case when user_type='new' then pay_item_num else 0 end) as new_pay_item_num ,sum(case when user_type='new' then pay_order_money else 0 end) as new_pay_order_money ,count(distinct (case when user_type='remain' then user_id else null end)) as remain_pay_uv ,sum(case when user_type='remain' then pay_order_num else 0 end) as remain_pay_order_num ,sum(case when user_type='remain' then pay_item_num else 0 end) as remain_pay_item_num ,sum(case when user_type='remain' then pay_order_money else 0 end) as remain_pay_order_money ,count(distinct (case when user_type='recall' then user_id else null end)) as recall_pay_uv ,sum(case when user_type='recall' then pay_order_num else 0 end) as recall_pay_order_num ,sum(case when user_type='recall' then pay_item_num else 0 end) as recall_pay_item_num ,sum(case when user_type='recall' then pay_order_money else 0 end) as recall_pay_order_money from temp.rpt_pangge_pay_order_daily_temp5 group by `date` ,city_market_id ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp7 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp7 as select `date` ,city_market_id ,count(distinct shop_id) as pay_shop_num ,count(distinct coo_shop_id) as coo_pay_shop_num ,sum(case when coo_shop_id is not null then pay_item_num else 0 end) as coo_pay_item_num from temp.rpt_pangge_pay_order_daily_temp1 group by `date` ,city_market_id ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp14 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp14 as select `date` ,city_market_id ,count(distinct (case when user_type='new' then user_id else null end)) as new_pay_uv ,count(distinct user_id) as pay_uv from temp.rpt_pangge_pay_order_daily_temp4 group by `date` ,city_market_id ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp15 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp15 as select `date` ,city_market_id ,new_pay_uv ,pay_uv ,sum(new_pay_uv) over (partition by city_market_id order by `date`) as pay_uv_sd from temp.rpt_pangge_pay_order_daily_temp14 ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp16 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp16 as select `date` ,city_market_id ,new_pay_uv ,pay_uv ,pay_uv_sd ,lag(pay_uv,1) over (partition by city_market_id order by `date`) as last_pay_uv ,lag(pay_uv_sd,1) over (partition by city_market_id order by `date`) as last_pay_uv_sd from temp.rpt_pangge_pay_order_daily_temp15 ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp8 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp8 as select `date` ,user_id ,sum(pay_order_num) as pay_order_num ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,sum(pay_item_money) as pay_item_money from temp.rpt_pangge_pay_order_daily_temp2 group by `date` ,user_id ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp9 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp9 as select * ,lag(`date`,1) over (partition by user_id order by `date`) as last_date from temp.rpt_pangge_pay_order_daily_temp8 ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp10 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp10 as select * ,(case when last_date is null then 'new' when datediff(`date`,last_date)=1 then 'remain' when datediff(`date`,last_date)>1 then 'recall' else null end) as user_type from temp.rpt_pangge_pay_order_daily_temp9 ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp11 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp11 as select t1.* from temp.rpt_pangge_pay_order_daily_temp10 t1 inner join temp.rpt_pangge_pay_order_daily_temp0 t2 on t1.`date`=t2.stati_date ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp12 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp12 as select `date` ,0 as city_market_id ,count(distinct user_id) as pay_uv ,sum(pay_order_num) as pay_order_num ,sum(pay_item_num) as pay_item_num ,sum(pay_order_money) as pay_order_money ,sum(pay_item_money) as pay_item_money ,count(distinct (case when user_type='new' then user_id else null end)) as new_pay_uv ,sum(case when user_type='new' then pay_order_num else 0 end) as new_pay_order_num ,sum(case when user_type='new' then pay_item_num else 0 end) as new_pay_item_num ,sum(case when user_type='new' then pay_order_money else 0 end) as new_pay_order_money ,count(distinct (case when user_type='remain' then user_id else null end)) as remain_pay_uv ,sum(case when user_type='remain' then pay_order_num else 0 end) as remain_pay_order_num ,sum(case when user_type='remain' then pay_item_num else 0 end) as remain_pay_item_num ,sum(case when user_type='remain' then pay_order_money else 0 end) as remain_pay_order_money ,count(distinct (case when user_type='recall' then user_id else null end)) as recall_pay_uv ,sum(case when user_type='recall' then pay_order_num else 0 end) as recall_pay_order_num ,sum(case when user_type='recall' then pay_item_num else 0 end) as recall_pay_item_num ,sum(case when user_type='recall' then pay_order_money else 0 end) as recall_pay_order_money from temp.rpt_pangge_pay_order_daily_temp11 group by `date` ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp13 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp13 as select `date` ,0 as city_market_id ,count(distinct shop_id) as pay_shop_num ,count(distinct coo_shop_id) as coo_pay_shop_num ,sum(case when coo_shop_id is not null then pay_item_num else 0 end) as coo_pay_item_num from temp.rpt_pangge_pay_order_daily_temp1 group by `date` ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp17 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp17 as select `date` ,count(distinct (case when user_type='new' then user_id else null end)) as new_pay_uv ,count(distinct user_id) as pay_uv from temp.rpt_pangge_pay_order_daily_temp4 group by `date` ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp18 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp18 as select `date` ,new_pay_uv ,pay_uv ,sum(new_pay_uv) over (order by `date`) as pay_uv_sd from temp.rpt_pangge_pay_order_daily_temp17 ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp19 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp19 as select `date` ,0 as city_market_id ,new_pay_uv ,pay_uv ,pay_uv_sd ,lag(pay_uv,1) over (order by `date`) as last_pay_uv ,lag(pay_uv_sd,1) over (order by `date`) as last_pay_uv_sd from temp.rpt_pangge_pay_order_daily_temp18 ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp20 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp20 as select t1.`date` ,t1.city_market_id ,t1.pay_uv ,t1.pay_order_num ,t1.pay_item_num ,t2.coo_pay_item_num ,t1.pay_order_money ,t1.pay_item_money ,t1.new_pay_uv ,t1.new_pay_order_num ,t1.new_pay_item_num ,t1.new_pay_order_money ,t1.remain_pay_uv ,t1.remain_pay_order_num ,t1.remain_pay_item_num ,t1.remain_pay_order_money ,t1.recall_pay_uv ,t1.recall_pay_order_num ,t1.recall_pay_item_num ,t1.recall_pay_order_money ,t2.pay_shop_num ,t2.coo_pay_shop_num ,t5.pay_uv_sd ,t5.last_pay_uv_sd ,t5.last_pay_uv from temp.rpt_pangge_pay_order_daily_temp6 t1 left join temp.rpt_pangge_pay_order_daily_temp7 t2 on t1.`date`=t2.`date` and t1.city_market_id=t2.city_market_id left join temp.rpt_pangge_pay_order_daily_temp16 t5 on t1.`date`=t5.`date` and t1.city_market_id=t5.city_market_id union all select t3.`date` ,t3.city_market_id ,t3.pay_uv ,t3.pay_order_num ,t3.pay_item_num ,t4.coo_pay_item_num ,t3.pay_order_money ,t3.pay_item_money ,t3.new_pay_uv ,t3.new_pay_order_num ,t3.new_pay_item_num ,t3.new_pay_order_money ,t3.remain_pay_uv ,t3.remain_pay_order_num ,t3.remain_pay_item_num ,t3.remain_pay_order_money ,t3.recall_pay_uv ,t3.recall_pay_order_num ,t3.recall_pay_item_num ,t3.recall_pay_order_money ,t4.pay_shop_num ,t4.coo_pay_shop_num ,t6.pay_uv_sd ,t6.last_pay_uv_sd ,t6.last_pay_uv from temp.rpt_pangge_pay_order_daily_temp12 t3 left join temp.rpt_pangge_pay_order_daily_temp13 t4 on t3.`date`=t4.`date` and t3.city_market_id=t4.city_market_id left join temp.rpt_pangge_pay_order_daily_temp19 t6 on t3.`date`=t6.`date` and t3.city_market_id=t6.city_market_id ;

drop table if exists temp.rpt_pangge_pay_order_daily_temp21 ;
create table if not exists temp.rpt_pangge_pay_order_daily_temp21 as select t1.`date` ,t1.city_market_id ,(case when t2.name is null then '总体' else t2.name end) as city_market_name ,t1.pay_uv ,t1.pay_order_num ,t1.pay_item_num ,t1.coo_pay_item_num ,t1.pay_order_money ,t1.pay_item_money ,t1.new_pay_uv ,t1.new_pay_order_num ,t1.new_pay_item_num ,t1.new_pay_order_money ,t1.remain_pay_uv ,t1.remain_pay_order_num ,t1.remain_pay_item_num ,t1.remain_pay_order_money ,t1.recall_pay_uv ,t1.recall_pay_order_num ,t1.recall_pay_item_num ,t1.recall_pay_order_money ,t1.pay_shop_num ,t1.coo_pay_shop_num ,t1.pay_uv_sd ,t1.last_pay_uv_sd ,t1.last_pay_uv from temp.rpt_pangge_pay_order_daily_temp20 t1 left join ods.t_city_market t2 on t1.city_market_id=t2.id ;

insert overwrite table rpt.rpt_pangge_pay_order_daily partition(`date`='${hivevar:day}') select city_market_id ,city_market_name ,pay_uv ,pay_order_num ,pay_item_num ,coo_pay_item_num ,pay_order_money ,pay_item_money ,new_pay_uv ,new_pay_order_num ,new_pay_item_num ,new_pay_order_money ,remain_pay_uv ,remain_pay_order_num ,remain_pay_item_num ,remain_pay_order_money ,recall_pay_uv ,recall_pay_order_num ,recall_pay_item_num ,recall_pay_order_money ,pay_shop_num ,coo_pay_shop_num ,pay_uv_sd ,last_pay_uv_sd ,last_pay_uv from temp.rpt_pangge_pay_order_daily_temp21 ;

