SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
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






drop table dws.dws_strength_bargain_item;

--实惠好货商品数据dws.dws_strength_bargain_item
create table dws.dws_strength_bargain_item as 
select t1.item_id,t1.shop_id,t1.city_market_id, t4.pay_item_num_3d ,t1.up_shelf_time,
row_number() over(order by  t4.pay_item_num_3d desc,t1.up_shelf_time  desc) rank   from 
(select id item_id,shop_id,city_market_id,discount_price,vcid,up_shelf_time from ods.t_item
where is_sp_front<100 and status=1) t1
 join
(select * from ods.t_shop_strength 
where status !=3 and end_time>='${hivevar:day}' and auth_time<='${hivevar:day}')t2
on t1.shop_id =t2.shop_id
left join
(select city_market_id,vcid,
percentile_approx(discount_price,0.1) point_1,percentile_approx(discount_price,0.33) point_2 
from ods.t_item 
group by city_market_id,vcid)t3
on t1.vcid=t3.vcid and t1.city_market_id=t3.city_market_id
left join
(select item_id,sum(pay_item_num_1d) pay_item_num_3d from dws.dws_pangge_item_allot_day
where `date`>=date_sub('${hivevar:day}',3) and `date`<='${hivevar:day}'
group by item_id)t4
on t1.item_id=t4.item_id
where t1.discount_price>t3.point_1 and t1.discount_price<=t3.point_2 and t4.pay_item_num_3d >=0

