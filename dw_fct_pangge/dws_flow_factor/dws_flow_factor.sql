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




----有效入仓率、缺货率
INSERT overwrite table dws.dws_flow_factor partition(`date`='${hivevar:day}')
select t2.city_market_code,t1.shop_id,
case when t1.mo_rate=0 then 0.01
else t1.mo_rate end  mo_rate_7d,
case when t1.shop_id =-1 then t3.min_mo_rate*2/3
else t1.mo_rate end flow_factor from
(select * from dws.dws_pangge_mo_rate_valid_shop 
where `date`= '${hivevar:day}' and mo_rate is not null)t1
left join
dim.dim_city_market t2
on t1.city_market_id=t2.city_market_id
left join
( select city_market_id,percentile_approx(mo_rate,0.5)  min_mo_rate  from dws.dws_pangge_mo_rate_valid_shop 
where `date`= '${hivevar:day}' and mo_rate is not null
group by city_market_id)t3
on t1.city_market_id=t3.city_market_id

