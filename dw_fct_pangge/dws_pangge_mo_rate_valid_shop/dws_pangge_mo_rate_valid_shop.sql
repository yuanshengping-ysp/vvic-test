set hive.tez.container.size=1024;
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled=false ;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
SET mapred.max.split.size=268435456; 
SET mapred.min.split.size.per.node=134217728 ; 
SET mapred.min.split.size.per.rack=134217728 ; 
SET hive.merge.mapfiles = true ;
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles = true ; 
SET hive.merge.size.per.task = 268435456 ;
SET hive.merge.smallfiles.avgsize=268435456 ;

---市场有效入仓率----
---日期传参是T-1
---T-1='${hivevar:day}'  '2020-11-13'
---T-4=date_sub('${hivevar:day}',7) '2020-11-10'
---T-31=date_sub('${hivevar:day}',36) '2020-10-12'

---计算分市场的下单件数、入仓件数、质检不合格换货的件数
---下单件数：dw.fct_pangge_pay_order_item的pay_item_num
---入仓件数：dw.fct_pangge_pay_order_item的order_details_id(剔重后)与ods.wms_product_label的ods_order_id进行关联，根据receving_flag=1的数量
---质检不合格换货件数：dw.fct_pangge_pay_order_item的order_details_id(剔重后)与ods.wms_product_label的ods_order_id进行关联，根据receving_flag=1并且product_label_status=9的数量
---下单日期：2020-10-11~2020-11-10
---入仓日期：2020-10-11~2020-11-13
---质检不合格换货：2020-10-11～2020-11-13

---计算档口在未关停状态下的下单量与下单数量
drop table temp.dws_pangge_shop_pay_day_temp01;
create table temp.dws_pangge_shop_pay_day_temp01 as 
select t1.* from 
(select * from dws.dws_pangge_shop_pay_day
where `date`<=date_sub('${hivevar:day}',7)) t1
left join
(select * from dwd.dwd_t_shop_punish_log_daily
where `date`= '${hivevar:day}')t2
on t1.shop_id=t2.shop_id
where t1.`date`>= substring(t2.last_time,1,10)
;

---计算每天档口在未关停状态下的入库量与入库数量(每天需要回溯过去30天的数据)
drop table temp.dws_pangge_shop_order_day_temp01;
create table temp.dws_pangge_shop_order_day_temp01 as 
select t1.* from 
(select * from dws.dws_pangge_shop_order_day
where `date`<=date_sub('${hivevar:day}',7)) t1
left join
(select * from dwd.dwd_t_shop_punish_log_daily
where `date`= '${hivevar:day}') t2
on t1.shop_id=t2.shop_id
where t1.`date`>= substring(t2.last_time,1,10)
;



----有效入仓率、缺货率
INSERT overwrite table dws.dws_pangge_mo_rate_valid_shop partition(`date`='${hivevar:day}')
---市场有效入仓率、缺货率
select t3.city_market_id,-1 shop_id,nvl(t4.receive_item_num,0)/t3.pay_item_num mo_rate
,nvl(t4.receive_item_num_2d,0)/t3.pay_item_num mo_rate_2d
,nvl(t4.ship_item_num_2d,0)/t3.pay_item_num ship_rate_2d
,(1-nvl(t4.receive_item_num,0)/t3.pay_item_num) mo_lack_rate 
,t4.receive_item_num receive_item_num
,t3.pay_item_num pay_item_num
,t4.receive_item_num_2d
,t4.ship_item_num_2d
,null d1
 from 
---下单件数
(select 
city_market_id,
sum(pay_item_num) pay_item_num
from temp.dws_pangge_shop_pay_day_temp01
where   `date`>=date_sub('${hivevar:day}',36) and  `date`<=date_sub('${hivevar:day}',7)  
group by city_market_id) t3
left join
---入仓件数  
(select city_market_id,sum(receive_item_num) receive_item_num,
sum(receive_item_num_2d) receive_item_num_2d,
sum(ship_item_num_2d) ship_item_num_2d
 from 
temp.dws_pangge_shop_order_day_temp01
where   `date`>=date_sub('${hivevar:day}',36) and  `date`<=date_sub('${hivevar:day}',7)  
group by city_market_id)t4
on t3.city_market_id=t4.city_market_id;

----分档口有效入仓率计算-----
---1.计算每个档口满足统计意义的最小订单数C1=(1/M0)*N，N取50；
---2.计算每个档口每天的下单数
---3.计算每个档口每天累计下单数（从最新日期2021-01-11开始往前累计）

drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp0;
---1.计算每个档口满足统计意义的最小订单数C1=(1/M0)*N，N取10；
create table temp.dws_pangge_mo_rate_valid_shop_temp0 as 
select city_market_id,(1/mo_lack_rate)*10 c1 from dws.dws_pangge_mo_rate_valid_shop
where  `date`='${hivevar:day}';

drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp1;
---计算每个档口以30天为周期的累计订单量
create table temp.dws_pangge_mo_rate_valid_shop_temp1 as 
select t4.*
from (select t3.city_market_id,t3.shop_id, t3.`date`,t3.pay_item_num
,sum(t3.pay_order_num ) over(partition by t3.`shop_id` order by cast(t3.`date` as date) desc) as pay_order_num_td
,sum(t3.pay_item_num ) over(partition by t3.`shop_id` order by cast(t3.`date` as date) desc) as pay_item_num_td
from 
( select t1.city_market_id,t1.shop_id, t1.`date`,
 case when t2.pay_item_num is null then 0 else t2.pay_item_num end pay_item_num,
  case when t2.pay_order_num is null then 0 else t2.pay_order_num end pay_order_num
 from 
 (select a.`date`,b.id shop_id,b.city_market_id
 from dw.dim_date a
 join
 ods.t_shop b
where a.`date`<=date_sub('${hivevar:day}',7)  
 ) t1
left join
(select * from 
  temp.dws_pangge_shop_pay_day_temp01
where `date`<=date_sub('${hivevar:day}',7)  ) t2
on t1.`date`=t2.`date`and t1.shop_id=t2.shop_id)t3)t4
where pmod(datediff(date_sub('${hivevar:day}',7)  ,t4.`date`),30)=29 ;

drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp2;
--存在【档口中累计下单数>=最小订单数C1的最大日期D1】
create table temp.dws_pangge_mo_rate_valid_shop_temp2 as 
select t1.shop_id,max(t1.`date`) d1 from 
temp.dws_pangge_mo_rate_valid_shop_temp1  t1
left join
temp.dws_pangge_mo_rate_valid_shop_temp0 t2
on t1.city_market_id=t2.city_market_id
-- where t1.pay_order_num_td>=t2.c1
where cast(t1.pay_item_num_td as double)>=cast(t2.c1 as double)
group by t1.shop_id;


drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp3;
----计算每个档口在【D1，2021-01-11】的下单件数

create table temp.dws_pangge_mo_rate_valid_shop_temp3 as 
select 
t1.shop_id,
sum(case when t1.`date`>=t2.d1 and t1.`date`<=date_sub('${hivevar:day}',7)   then t1.pay_item_num else 0 end ) pay_item_num
 from 
temp.dws_pangge_shop_pay_day_temp01 t1
left join
 temp.dws_pangge_mo_rate_valid_shop_temp2 t2
 on t1.shop_id=t2.shop_id
 group by t1.shop_id;
 

drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp4;
--计算上述【i】中的下单商品在【D1，2021-01-17】的入仓件数、质检不合格换货件数
create table temp.dws_pangge_mo_rate_valid_shop_temp4 as 
select 
t1.city_market_id,
t1.shop_id,
sum(case when t1.`date`>=t2.d1 and t1.`date`<=date_sub('${hivevar:day}',7)    then t1.receive_item_num else 0 end ) receive_item_num ,
sum(case when t1.`date`>=t2.d1 and t1.`date`<=date_sub('${hivevar:day}',7)    then t1.receive_item_num_2d else 0 end ) receive_item_num_2d,
sum(case when t1.`date`>=t2.d1 and t1.`date`<=date_sub('${hivevar:day}',7)    then t1.ship_item_num_2d else 0 end ) ship_item_num_2d,
t2.d1
 from 
temp.dws_pangge_shop_order_day_temp01 t1
left join
 temp.dws_pangge_mo_rate_valid_shop_temp2 t2
 on t1.shop_id=t2.shop_id
 group by t1.city_market_id,t1.shop_id,t2.d1;
 
 
 ---计算档口的有效入仓率M1
 insert into table dws.dws_pangge_mo_rate_valid_shop partition(`date`='${hivevar:day}')
select t1.city_market_id,t1.shop_id,nvl(t1.receive_item_num,0)/t2.pay_item_num mo_rate ,
nvl(t1.receive_item_num_2d,0)/t2.pay_item_num mo_rate_2d,
nvl(t1.ship_item_num_2d,0)/t2.pay_item_num ship_rate_2d,
1-nvl(t1.receive_item_num,0)/t2.pay_item_num mo_lack_rate ,
t1.receive_item_num,
t2.pay_item_num,
t1.receive_item_num_2d,
t1.ship_item_num_2d,
t1.d1
 from temp.dws_pangge_mo_rate_valid_shop_temp4 t1
 left join
 temp.dws_pangge_mo_rate_valid_shop_temp3 t2
 on t1.shop_id=t2.shop_id
 where t1.receive_item_num>0 or t2.pay_item_num>0 
;
 
 ---若档口有效入仓率M1<0.5*M0，则C1>=(1/M1)*N,N=10，再次循环a的步聚
drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp5;
create table temp.dws_pangge_mo_rate_valid_shop_temp5 as 
select t1.* from 
 (select * from dws.dws_pangge_mo_rate_valid_shop
 where shop_id !=-1 and `date`='${hivevar:day}') t1
 left join
 (select * from dws.dws_pangge_mo_rate_valid_shop
 where shop_id =-1 and `date`='${hivevar:day}') t2
 on t1.city_market_id=t2.city_market_id
 where t1.mo_lack_rate<t2.mo_lack_rate*0.5;

 
drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp6;
----C1>=(1/M1)*N,N=10
create table temp.dws_pangge_mo_rate_valid_shop_temp6 as 
select shop_id,(1/mo_lack_rate)*10 c1 from temp.dws_pangge_mo_rate_valid_shop_temp5;

drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp7;
------重新跑一次循环
--存在【档口中累计下单数>=最小订单数C1的最大日期D1】
create table temp.dws_pangge_mo_rate_valid_shop_temp7 as 
select t1.shop_id,max(t1.`date`) d1 from 
temp.dws_pangge_mo_rate_valid_shop_temp1  t1
 join
temp.dws_pangge_mo_rate_valid_shop_temp6 t2
on t1.shop_id=t2.shop_id
-- where t1.pay_order_num_td>=t2.c1
where cast(t1.pay_item_num_td as double)>= cast(t2.c1 as double)
group by t1.shop_id;


----计算每个档口在【D1，2021-01-11】的下单件数
drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp8;
create table temp.dws_pangge_mo_rate_valid_shop_temp8 as 
select 
t1.shop_id,
sum(case when t1.`date`>=t2.d1 and t1.`date`<=date_sub('${hivevar:day}',7)   then t1.pay_item_num else 0 end ) pay_item_num
 from 
temp.dws_pangge_shop_pay_day_temp01 t1
 join
 temp.dws_pangge_mo_rate_valid_shop_temp7 t2
 on t1.shop_id=t2.shop_id
 group by t1.shop_id;
 

--计算上述【i】中的下单商品在【D1，2021-01-17】的入仓件数、质检不合格换货件数
drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp9;
create table temp.dws_pangge_mo_rate_valid_shop_temp9 as 
select 
t1.city_market_id,
t1.shop_id,
sum(case when t1.`date`>=t2.d1 and t1.`date`<=date_sub('${hivevar:day}',7)    then t1.receive_item_num else 0 end ) receive_item_num,
sum(case when t1.`date`>=t2.d1 and t1.`date`<=date_sub('${hivevar:day}',7)    then t1.receive_item_num_2d else 0 end ) receive_item_num_2d,
sum(case when t1.`date`>=t2.d1 and t1.`date`<=date_sub('${hivevar:day}',7)    then t1.ship_item_num_2d else 0 end ) ship_item_num_2d,
t2.d1
 from 
temp.dws_pangge_shop_order_day_temp01 t1
 join
 temp.dws_pangge_mo_rate_valid_shop_temp7 t2
 on t1.shop_id=t2.shop_id
 group by t1.city_market_id,t1.shop_id,t2.d1;

  
 ---计算档口的有效入仓率M1(第二次插入入仓率低的档口)
INSERT overwrite table dws.dws_pangge_mo_rate_valid_shop partition(`date`='${hivevar:day}')
 select  t.city_market_id,t.shop_id,t.mo_rate,
 t.mo_rate_2d,
 t.ship_rate_2d,
 t.mo_lack_rate  ,
 t.receive_item_num,
 t.pay_item_num,
 t.receive_item_num_2d,
 t.ship_item_num_2d,
 t.d1
 from dws.dws_pangge_mo_rate_valid_shop t
 where `date`='${hivevar:day}'
 and shop_id not in(
 select shop_id from temp.dws_pangge_mo_rate_valid_shop_temp5)
 union all
 select cast(t1.city_market_id as int) city_market_id,t1.shop_id,nvl(t1.receive_item_num,0)/t2.pay_item_num mo_rate  ,
nvl(t1.receive_item_num_2d,0)/t2.pay_item_num mo_rate_2d,
nvl(t1.ship_item_num_2d,0)/t2.pay_item_num ship_rate_2d,
1-nvl(t1.receive_item_num,0)/t2.pay_item_num mo_lack_rate ,
t1.receive_item_num,
t2.pay_item_num,
t1.receive_item_num_2d,
t1.ship_item_num_2d,
t1.d1
 from temp.dws_pangge_mo_rate_valid_shop_temp9 t1
  join
 temp.dws_pangge_mo_rate_valid_shop_temp8 t2
 on t1.shop_id=t2.shop_id;


drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp10;
---计算档口截至2021-01-11的下单件数，下单量K
create table temp.dws_pangge_mo_rate_valid_shop_temp10 as 
select t1.shop_id,t1.city_market_id,t2.pay_item_num from 
(select t.id shop_id,t.city_market_id from 
ods.t_shop t
where  t.id not in 
(select shop_id from  dws.dws_pangge_mo_rate_valid_shop where `date`= '${hivevar:day}')
) t1
left join
(select shop_id,sum(pay_item_num) pay_item_num from temp.dws_pangge_shop_pay_day_temp01
-- where `date`<date_sub('${hivevar:day}',7) 
where `date`<=date_sub('${hivevar:day}',7) 
group by shop_id) t2
on t1.shop_id=t2.shop_id;
 

---计算上述【i】的下单商品在2021-01-17之前(含当日)的入仓件数、质检不合格换货件数  ---
drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp12;
create table temp.dws_pangge_mo_rate_valid_shop_temp12 as 
select t1.shop_id,t1.city_market_id,t2.receive_item_num,t2.receive_item_num_2d,t2.ship_item_num_2d from 
(select t.id shop_id,t.city_market_id from 
ods.t_shop t
where t.id not in 
(select shop_id from  dws.dws_pangge_mo_rate_valid_shop where `date`= '${hivevar:day}')
) t1
-- left join
inner join 
(select shop_id,sum(receive_item_num) receive_item_num,
 sum(receive_item_num_2d)receive_item_num_2d,
 sum(ship_item_num_2d) ship_item_num_2d
 from temp.dws_pangge_shop_order_day_temp01
-- where `date`<'${hivevar:day}'
where `date`<=date_sub('${hivevar:day}',7) 
group by shop_id) t2
on t1.shop_id=t2.shop_id;


---计算档口的有效入仓率M1  ---
drop table if exists temp.dws_pangge_mo_rate_valid_shop_temp13;
create table temp.dws_pangge_mo_rate_valid_shop_temp13 as 
 select t1.city_market_id,t1.shop_id,nvl(t2.receive_item_num,0)/t1.pay_item_num mo_rate ,
nvl(t2.receive_item_num_2d,0)/t1.pay_item_num mo_rate_2d,
nvl(t2.ship_item_num_2d,0)/t1.pay_item_num ship_rate_2d,
1-nvl(t2.receive_item_num,0)/t1.pay_item_num mo_lack_rate ,
t2.receive_item_num,
t1.pay_item_num,
t2.receive_item_num_2d,
t2.ship_item_num_2d,
 'history' d1
 from temp.dws_pangge_mo_rate_valid_shop_temp10 t1
 left join
 temp.dws_pangge_mo_rate_valid_shop_temp12 t2
 on t1.shop_id=t2.shop_id;


---计算档口最终有效入仓率M1=(M1*K+M0*(C1-K))/C1 ---
 insert into table dws.dws_pangge_mo_rate_valid_shop partition(`date`= '${hivevar:day}')
select t1.city_market_id,t1.shop_id,
case when cast(t2.pay_item_num as double) <t4.c1 then 
(t1.mo_rate*t2.pay_item_num+t3.mo_rate*(t4.c1-t2.pay_item_num))/t4.c1
else t1.mo_rate end mo_rate ,
t1.mo_rate_2d,
t1.ship_rate_2d,
case when cast(t2.pay_item_num as double) <t4.c1 then 
(t1.mo_lack_rate*t2.pay_item_num+t3.mo_lack_rate*(t4.c1-t2.pay_item_num))/t4.c1
else t1.mo_lack_rate end  mo_lack_rate,
t1.receive_item_num,
t1.pay_item_num,
t1.receive_item_num_2d,
t1.ship_item_num_2d,
case when cast(t2.pay_item_num as double) <t4.c1 
then '-1'
else t1.d1  end d1
 from temp.dws_pangge_mo_rate_valid_shop_temp13 t1
 left join
 temp.dws_pangge_mo_rate_valid_shop_temp10 t2
 on t1.shop_id=t2.shop_id
 left join
  (select * from dws.dws_pangge_mo_rate_valid_shop 
 where `date`='${hivevar:day}' and shop_id=-1) t3
 on t1.city_market_id=t3.city_market_id
 left join
 temp.dws_pangge_mo_rate_valid_shop_temp0 t4
 on t1.city_market_id=t4.city_market_id;
 


 
