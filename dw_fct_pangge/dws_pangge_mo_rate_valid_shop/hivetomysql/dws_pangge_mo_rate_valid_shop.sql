SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
SET hive.tez.container.size=1024;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false ;

drop table if exists temp.dws_pangge_mo_rate_valid_shop;

create table temp.dws_pangge_mo_rate_valid_shop as 
select  
city_market_id,
shop_id,
mo_rate mo_rate_7d,
mo_rate_2d,
ship_rate_2d,
mo_lack_rate mo_lack_rate_7d,
receive_item_num receive_item_num_7d,
pay_item_num pay_item_num_7d,
receive_item_num_2d,
ship_item_num_2d,
d1,
`date`
from dws.dws_pangge_mo_rate_valid_shop
where `date`='${hivevar:day}'