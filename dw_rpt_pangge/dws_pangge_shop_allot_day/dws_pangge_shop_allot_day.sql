SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;



---档口备货率----
DROP TABLE IF EXISTS temp.dws_pangge_shop_allot_day_temp0;

CREATE TABLE temp.dws_pangge_shop_allot_day_temp0 AS 
SELECT s1.city_market_id,s1.shop_id,s1.allot_ratio_2w,s1.pay_item_num_2w, s1.rank,s1.shop_num,s2.allot_ratio_1d,s2.pay_item_num_1d 
--14天备货率
from (select t1.city_market_id,t1.shop_id,t1.shop_name
,(case when t2.shop_id is null then 0 else 1 end) as is_coo_shop
,t2.allot_ratio allot_ratio_2w
,rank() over(partition by t1.city_market_id order by (case when t2.allot_ratio is null then 0 else t2.allot_ratio end) desc) as rank
,count(1) over(partition by t1.city_market_id) as shop_num
,nvl(t4.pay_item_num,0) as pay_item_num_2w,'${hivevar:day}' `date`
from (select id as shop_id
        ,city_market_id
        ,name as shop_name
        from ods.t_shop
        where status=1) t1
    left join
    --新口径备货率
        (select shop_id
            ,sum(allot_item_num)/sum(enallot_item_num+print_item_num) as allot_ratio
        from dws.dws_pangge_shop_allot_daily
        where shop_id != -1
            and `date`>=date_sub('${hivevar:day}',14) and `date`<='${hivevar:day}'
        group by shop_id) t2
    on t1.shop_id=t2.shop_id
    left join
    -- 下单支付件数
        (select shop_id
            ,sum(pay_item_num) as pay_item_num
        from dw.fct_pangge_pay_order_item
        where `date`>=date_sub('${hivevar:day}',14) and `date`<='${hivevar:day}'
        group by shop_id) t4
    on t1.shop_id=t4.shop_id) s1
    left join
    (select t1.city_market_id
        ,t1.shop_id
        ,t1.shop_name
        ,(case when t2.shop_id is null then 0 else 1 end) as is_coo_shop
        ,t2.allot_ratio allot_ratio_1d
        ,rank() over(partition by t1.city_market_id order by (case when t2.allot_ratio is null then 0 else t2.allot_ratio end) desc) as rank
        ,count(1) over(partition by t1.city_market_id) as shop_num
        ,nvl(t4.pay_item_num,0) as pay_item_num_1d
    from (select id as shop_id
            ,city_market_id
            ,name as shop_name
            from ods.t_shop
            where status=1) t1
    left join
    --新口径备货率
        (select shop_id
            ,sum(allot_item_num)/sum(enallot_item_num+print_item_num) as allot_ratio
        from dws.dws_pangge_shop_allot_daily
        where shop_id != -1
            and `date`='${hivevar:day}'
        group by shop_id) t2
    on t1.shop_id=t2.shop_id
    left join
    -- 下单支付件数
        (select shop_id
            ,sum(pay_item_num) as pay_item_num
        from dw.fct_pangge_pay_order_item
        where `date`='${hivevar:day}'
        group by shop_id) t4
    on t1.shop_id=t4.shop_id) s2
on s1.shop_id=s2.shop_id
;


INSERT overwrite table dws.dws_pangge_shop_allot_day partition(`date`='${hivevar:day}')
select city_market_id,shop_id,allot_ratio_2w,pay_item_num_2w,
case when pay_item_num_2w<10 then null
when pay_item_num_2w>=10 and rank*1.0/shop_num<=0.49 then round(0.5-rank*1.0/shop_num,4)
when pay_item_num_2w>=10 and rank*1.0/shop_num<=0.51 then 0
else round(0.5-rank*1.0/shop_num,4) end as `opp_allot_ratio_2w` ,
allot_ratio_1d,pay_item_num_1d 
FROM
temp.dws_pangge_shop_allot_day_temp0

union all
select s3.city_market_id,-1 shop_id,s3.allot_ratio_2w,s3.pay_item_num_2w,0.0 opp_allot_ratio_2w,
s4.allot_ratio_1d,s4.pay_item_num_1d
from 
(select t1.city_market_id ,-1 shop_id ,t1.allot_ratio allot_ratio_2w  ,nvl(t2.pay_item_num,0) as pay_item_num_2w
from
--新口径备货率
(select city_market_id,
 sum(allot_item_num)/sum(enallot_item_num+print_item_num) as allot_ratio
from dws.dws_pangge_shop_allot_daily
where shop_id = -1 and  `date`>=date_sub('${hivevar:day}',14) and `date`<='${hivevar:day}'
group by city_market_id) t1
left join
-- 下单支付件数
(select city_market_id  ,sum(pay_item_num) as pay_item_num
from dw.fct_pangge_pay_order_item
where  `date`>=date_sub('${hivevar:day}',14) and `date`<='${hivevar:day}'
group by city_market_id) t2
on t1.city_market_id=t2.city_market_id) s3
left join
(select t1.city_market_id ,-1 shop_id ,t1.allot_ratio allot_ratio_1d  ,nvl(t2.pay_item_num,0) as pay_item_num_1d
from
--新口径备货率
(select city_market_id,
 sum(allot_item_num)/sum(enallot_item_num+print_item_num) as allot_ratio
from dws.dws_pangge_shop_allot_daily
where shop_id = -1 and `date`='${hivevar:day}'
group by city_market_id) t1
left join
-- 下单支付件数
(select city_market_id  ,sum(pay_item_num) as pay_item_num
from dw.fct_pangge_pay_order_item
where `date`='${hivevar:day}'
group by city_market_id) t2
on t1.city_market_id=t2.city_market_id) s4
on s3.city_market_id=s4.city_market_id
;




    
