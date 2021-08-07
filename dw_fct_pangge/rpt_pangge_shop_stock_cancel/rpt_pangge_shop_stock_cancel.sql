SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

set hive.tez.container.size=1024;
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled=false ;

ALTER TABLE rpt.rpt_pangge_shop_stock_cancel DROP IF EXISTS PARTITION (`date`='${hivevar:day}');
insert into table rpt.rpt_pangge_shop_stock_cancel partition(`date`='${hivevar:day}')
select t4.shop_id,t4.receive_item_num_2d,t4.receive_item_num_ratio_2d,
t4.pay_item_num,t4.cancel_item_num_2w,
t4.cancel_item_num_ratio_2d,
rank() over(partition by t4.city_market_id order by t4.pay_item_num desc) pay_item_rank,
rank() over(partition by t4.city_market_id order by t4.cancel_item_num_ratio_2d ) cancel_item_num_ratio_2d_rank,
rank() over(partition by t4.city_market_id order by t4.receive_item_num_ratio_2d desc) receive_item_num_ratio_2d_rank
from (
--近14天已备货件数，近14天48h备货率--   --近14天取消率=近14天取消件数/近14天交易件数
select t1.city_market_id,t1.shop_id,t1.receive_item_num_2d,t1.receive_item_num_ratio_2d,
t2.pay_item_num,t3.cancel_item_num_2w,
(case when t3.cancel_item_num_2w is null then 0 else  t3.cancel_item_num_2w end )/t2.pay_item_num cancel_item_num_ratio_2d
from 
(select city_market_id,shop_id,receive_item_num_2d,receive_item_num_ratio_2d from  rpt.rpt_pangge_shop_stock2
 where  `date`='${hivevar:day}') t1
left join
--近14天交易件数--
 (select shop_id,count(1) pay_item_num from dw.dim_wms_product_label
where label_type in (0,1) and order_type=0 and pay_time>=date_sub('${hivevar:day}',14) and pay_time<'${hivevar:day}'
group by shop_id) t2
on t1.shop_id=t2.shop_id
left join
 --近14天取消件数
 (select a.shop_id,count(1) cancel_item_num_2w from 
(select order_details_id,product_label_no,shop_id from dw.dim_wms_product_label ) a
join 
(select distinct order_details_id from dw.dim_wms_product_label
where  pay_time>=date_sub('${hivevar:day}',14) and pay_time<'${hivevar:day}') b
on a.order_details_id=b.order_details_id
 join 
(select product_label_no from ods.wms__wms_product_label_log
 where current_life_status=9 and event_type<>'098')c
 on a.product_label_no=c.product_label_no
 group by a.shop_id )t3
 on t1.shop_id=t3.shop_id) t4


