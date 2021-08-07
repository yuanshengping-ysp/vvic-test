SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
SET mapreduce.map.memory.mb=10150; 
SET mapreduce.map.java.opts=-Xmx6144m;
SET mapreduce.reduce.memory.mb=10150; 
SET mapreduce.reduce.java.opts=-Xmx8120m;


   -------商品备货率------
INSERT overwrite table dws.dws_pangge_item_allot_day partition(`date`='${hivevar:day}')
select s1.city_market_id,s1.shop_id,s1.item_id,s2.allot_ratio_1d,s2.pay_item_num_1d,s3.allot_ratio_1w,s3.pay_item_num_1w,s1.allot_ratio_2w,s1.pay_item_num_2w
--14天备货率
from (select t1.city_market_id,t1.item_id,t1.shop_id
,t2.allot_ratio allot_ratio_2w
,nvl(t4.pay_item_num,0) as pay_item_num_2w
from (select id as item_id
		,city_market_id,shop_id
		from ods.t_item
		where status=1) t1
    left join
    --新口径备货率
        (select item_id
            ,sum(allot_item_num)/sum(enallot_item_num+print_item_num) as allot_ratio
        from dws.dws_pangge_item_allot_daily
        where item_id != -1
            and `date`>=date_sub('${hivevar:day}',14) and `date`<='${hivevar:day}'
        group by item_id) t2
    on t1.item_id=t2.item_id
    left join
    -- 下单支付件数
        (select item_id
            ,sum(pay_item_num) as pay_item_num
        from dw.fct_pangge_pay_order_item
        where `date`>=date_sub('${hivevar:day}',14) and `date`<='${hivevar:day}'
        group by item_id) t4
    on t1.item_id=t4.item_id) s1
left join
(select t1.item_id,t2.allot_ratio allot_ratio_1d
,nvl(t4.pay_item_num,0) as pay_item_num_1d
from (select id as item_id
		,city_market_id
		from ods.t_item
		where status=1) t1
    left join
    --新口径备货率
        (select item_id
            ,sum(allot_item_num)/sum(enallot_item_num+print_item_num) as allot_ratio
        from dws.dws_pangge_item_allot_daily
        where item_id != -1
            and `date`='${hivevar:day}'
        group by item_id) t2
    on t1.item_id=t2.item_id
    left join
    -- 下单支付件数
        (select item_id
            ,sum(pay_item_num) as pay_item_num
        from dw.fct_pangge_pay_order_item
        where `date`='${hivevar:day}'
        group by item_id) t4
    on t1.item_id=t4.item_id) s2
on s1.item_id=s2.item_id
left join
(select t1.item_id,t2.allot_ratio allot_ratio_1w
,nvl(t4.pay_item_num,0) as pay_item_num_1w
from (select id as item_id
		,city_market_id
		from ods.t_item
		where status=1) t1
    left join
    --新口径备货率
        (select item_id
            ,sum(allot_item_num)/sum(enallot_item_num+print_item_num) as allot_ratio
        from dws.dws_pangge_item_allot_daily
        where item_id != -1
            and   `date`>=date_sub('${hivevar:day}',7) and `date`<='${hivevar:day}'
        group by item_id) t2
    on t1.item_id=t2.item_id
    left join
    -- 下单支付件数
        (select item_id
            ,sum(pay_item_num) as pay_item_num
        from dw.fct_pangge_pay_order_item
        where   `date`>=date_sub('${hivevar:day}',7) and `date`<='${hivevar:day}'
        group by item_id) t4
    on t1.item_id=t4.item_id) s3
on s1.item_id=s3.item_id