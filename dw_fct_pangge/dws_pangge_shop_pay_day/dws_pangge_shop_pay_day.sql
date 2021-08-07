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




---计算每天档口的下单量与下单数量
INSERT overwrite table dws.dws_pangge_shop_pay_day partition(`date`='${hivevar:day}')
select city_market_id,shop_id,sum(pay_item_num) pay_item_num,count(distinct order_no) pay_order_num
from dw.fct_pangge_pay_order_item
where `date`='${hivevar:day}'
group by shop_id,city_market_id;




