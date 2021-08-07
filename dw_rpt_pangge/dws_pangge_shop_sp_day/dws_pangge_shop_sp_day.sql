SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

----档口实拍率-----
INSERT overwrite table dws.dws_pangge_shop_sp_day partition(`date`='${hivevar:day}')
select s1.city_market_id,s1.shop_id,
case when s1.item_num<10 then null
when s1.item_num>=10 and s1.rank*1.0/s1.shop_num<=0.49 then round(0.5-s1.rank*1.0/s1.shop_num,4)
when s1.item_num>=10 and s1.rank*1.0/s1.shop_num<=0.51 then 0
else concat(round(0.5-s1.rank*1.0/s1.shop_num,4)) end as `opp_sp_ratio` ,s1.sp_ratio,s1.item_num,s1.item_num_sp
from (
 select t1.city_market_id,t1.shop_id,t1.sp_ratio,t1.item_num,t1.item_num_sp 
,rank() over(partition by t1.city_market_id order by (case when t1.sp_ratio is null then 0 else t1.sp_ratio end) desc) as rank
,count(1) over(partition by t1.city_market_id) as shop_num
from
(select city_market_id,shop_id,count(1) item_num ,count(case when is_sp_front<100 then 1
else null end ) item_num_sp,
count(case when is_sp_front<100 then 1
else null end )/count(1) sp_ratio from ods.t_item  
where  status=1 and spam=0
group by city_market_id,shop_id)t1
)s1