
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

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.created.files=10000;

-- 步骤1：创建表
create table if not exists dws.dws_recsys_recall_shop_daily ( `shop_id` bigint comment '档口id' ,`city_market_id` int comment '市场站点id' ,`shop_status` int comment '档口状态' ,`roi_shop_type` int comment '档口类型' ,`roi_res` int comment 'roi召回结果' ,`roi_factor` float comment 'roi权重因子' ,`pay_order_money_14d` float comment '近14天下单金额' ,`pangge_pay_order_money_14d` float comment '近14天搜款网下单金额' ,`shopdf_pay_order_money_14d` float comment '近14天档口代发下单金额' ,`is_live_shop` float comment '是否直播频道档口' ,`pay_order_money_rank_14d` bigint comment '近14天下单金额排名' ,`is_recall_uptime` int comment '上新实验组是否召回' ,`is_recall_roi` int comment 'roi实验组是否召回' ) comment '推荐系统召回档口表' partitioned by (`date` string comment '统计日期' ) stored as parquet location 'hdfs://master:8020/user/hive/warehouse/dws.db/dws_recsys_recall_shop_daily';
-- 步骤2.0：生成统计日期
drop table if exists temp.dws_recsys_recall_shop_daily_temp0 ;
create table if not exists temp.dws_recsys_recall_shop_daily_temp0 as select cast(date_add(`start_date`, `pos`) as string) as `stati_date` from (select '${hivevar:day}' as `start_date` ,'${hivevar:day}' as `end_date` ) t1 lateral view posexplode(split(space(datediff(`end_date`, `start_date`)), '')) t2 as pos, val  ;

-- 近14天代发交易额TopN的档口 
drop table if exists temp.dws_recsys_recall_shop_daily_temp1 ;
create table if not exists temp.dws_recsys_recall_shop_daily_temp1 as select s2.`stati_date` as `date` ,s1.`shop_id` ,sum(s1.`pay_order_money`) as `pay_order_money_14d` ,sum(case when s1.`type`='pangge' then s1.`pay_order_money` else 0 end) as `pangge_pay_order_money_14d` ,sum(case when s1.`type`='shopdf' then s1.`pay_order_money` else 0 end) as `shopdf_pay_order_money_14d` from (select `date` ,`shop_id` ,'pangge' as `type` ,sum(`sales_money`) as `pay_order_money` from dw.fct_shop_data where `date`>date_sub('${hivevar:day}',14) and `date`<='${hivevar:day}' group by `date` ,`shop_id` union all select t1.`date` ,t1.`shop_id` ,'shopdf' as `type` ,sum(t1.`df_pay_order_money`) as `pay_order_money` from (select distinct `date` ,`shop_id` ,`order_no` ,`df_pay_order_money` from dwd.dwd_shopdf_pay_order_detail where `date`>date_sub('${hivevar:day}',14) and `date`<='${hivevar:day}' ) t1 left join dwd.dwd_shopdf_antispam_detail t2 on t1.`order_no`=t2.`order_no` where t2.`order_no` is null group by t1.`date` ,t1.`shop_id`) s1 ,temp.dws_recsys_recall_shop_daily_temp0 s2 where s1.`date`>date_sub(s2.`stati_date`,14) and s1.`date`<=s2.`stati_date` group by s2.`stati_date` ,s1.`shop_id`;

-- 整合roi和直播档口数据 
drop table if exists temp.dws_recsys_recall_shop_daily_temp2 ;
create table if not exists temp.dws_recsys_recall_shop_daily_temp2 as select t1.`date` ,t1.`shop_id` ,t1.`city_market_id` ,t1.`roi_shop_type` ,t1.`roi_res` ,t1.`roi_factor` ,nvl(t2.`pay_order_money_14d`,0) as `pay_order_money_14d` ,nvl(t2.`pangge_pay_order_money_14d`,0) as `pangge_pay_order_money_14d` ,nvl(t2.`shopdf_pay_order_money_14d`,0) as `shopdf_pay_order_money_14d` ,(case when t3.`shop_id` is not null then 1 else 0 end) as `is_live_shop` ,rank() over(partition by t1.`city_market_id` order by nvl(t2.`pay_order_money_14d`,0) desc) as `pay_order_money_rank_14d` from (select * from dws.dws_plat_shop_roi_day where `date`>='${hivevar:day}' and `date`<='${hivevar:day}') t1 left join temp.dws_recsys_recall_shop_daily_temp1 t2 on t1.`shop_id`=t2.`shop_id` and t1.`date`=t2.`date` left join (select distinct `date` ,`shop_id` from dws.dws_plat_live_item_day where `date`>='${hivevar:day}' and `date`<='${hivevar:day}') t3 on t1.`shop_id`=t3.`shop_id` and t1.`date`=t3.`date` ;

-- 判断是否要召回 
drop table if exists temp.dws_recsys_recall_shop_daily_temp3 ;
create table if not exists temp.dws_recsys_recall_shop_daily_temp3 as select t1.* ,t2.`status` as `shop_status` ,(case when t2.`status`<>1 then 0 else 1 end) as `is_recall_uptime` ,(case when t2.`status`<>1 then 0 when t1.`roi_res`=1 then 0 when t1.`roi_res`=0 then 1 when t1.`roi_res`=-1 and t1.`is_live_shop`=1 then 1 when t1.`roi_res`=-1 and t1.`city_market_id`=1 and t1.`pay_order_money_rank_14d`<=3000 then 1 else 0 end) as `is_recall_roi` from temp.dws_recsys_recall_shop_daily_temp2 t1 left join ods.t_shop t2 on t1.`shop_id`=t2.`id` ;

-- 结果数据写入

insert overwrite table dws.dws_recsys_recall_shop_daily partition(`date`) select `shop_id` ,`city_market_id` ,`shop_status` ,`roi_shop_type` ,`roi_res` ,`roi_factor` ,`pay_order_money_14d` ,`pangge_pay_order_money_14d` ,`shopdf_pay_order_money_14d` ,`is_live_shop` ,`pay_order_money_rank_14d` ,`is_recall_uptime` ,`is_recall_roi` ,`date` from temp.dws_recsys_recall_shop_daily_temp3 ;




