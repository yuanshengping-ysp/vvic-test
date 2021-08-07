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


---计算所有档口复通的时间
INSERT overwrite table dwd.dwd_t_shop_punish_log_daily partition(`date`='${hivevar:day}')
select t1.id shop_id,
case when t2.create_time is null then '2000-01-01'
else t2.create_time end  last_time
from ods.t_shop t1
left join
(select shop_id,max(create_time) create_time from ods.t_shop_punish_log
where action in (3,5)  group by shop_id)t2
on t1.id=t2.shop_id;

