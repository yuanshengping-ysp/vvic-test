
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



drop table temp.dws_plat_shop_roi_day;
create table temp.dws_plat_shop_roi_day as
select t1.city_market_id
,t1.shop_id
,t1.roi_shop_type
,t1.shop_roi_2w
,t1.total_cost_amt_2w
,t1.upload_num_2w
,t1.upload_user_num_2w
,t1.fav_item_num_2w
,t1.fav_item_user_num_2w
,t1.fav_shop_num_2w
,t1.fav_shop_user_num_2w
,t1.datapack_down_num_2w
,t1.datapack_down_user_num_2w
,t1.median_down_num_2w
,t1.median_down_user_num_2w
,t1.addcart_num_2w
,t1.addcart_user_num_2w
,t2.roi_res
,t2.roi_factor
,t1.`date`
from (select *
from dws.dws_plat_shop_roi_day 
where `date`='${hivevar:day}' ) t1
left join 
(select *
from dws.dws_recsys_recall_shop_daily
where `date`='${hivevar:day}') t2 
on t1.`date`=t2.`date`
and t1.`shop_id`=t2.`shop_id`;





