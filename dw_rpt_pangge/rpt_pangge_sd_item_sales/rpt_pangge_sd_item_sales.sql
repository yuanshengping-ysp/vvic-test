SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.tez.container.size=1024;
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled=false ;


drop table if exists rpt.rpt_pangge_sd_item_sales;
--类目销量表--仅统计合作档口的商品的3天7天的销量
create table rpt.rpt_pangge_sd_item_sales as
select t2.item_id,t2.shop_id,t1.sales_num_3d,t2.discount_price,t2.vcid,t4.ad_cate_id,t5.allot_ratio_1w,t5.pay_item_num_1w,t2.title,t2.index_img_url,'${hivevar:day}' `date` from 
(select id item_id,shop_id,vcid,city_market_id,discount_price,title,index_img_url from ods.t_item
where is_lack =0 and is_pre_sell=0 and status=1) t2
 join
 (select shop_id from dw.fct_cooperate_shop 
 where `date`='${hivevar:day}' and coo_shop_num=1)t3
 on t2.shop_id  =t3.shop_id
left join
(select item_id,shop_id,sum(sales_num) sales_num_3d from dw.fct_item_data 
where `date`>=date_sub('${hivevar:day}',3) and `date`<='${hivevar:day}'  
group by item_id,shop_id
order by sales_num_3d desc) t1
 on t1.item_id=t2.item_id
left join
ods.t_ad_cate_vcid  t4
on t4.city_market_id=t2.city_market_id and t4.vcid=t2.vcid
left join
(select * from dws.dws_pangge_item_allot_day
 where `date`='${hivevar:day}') t5
on t2.item_id=t5.item_id


