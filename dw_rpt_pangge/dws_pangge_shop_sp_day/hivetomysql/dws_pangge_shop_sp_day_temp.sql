SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table temp.dws_pangge_shop_sp_day;
create table temp.dws_pangge_shop_sp_day as 
select city_market_id,shop_id,opp_sp_ratio opp_sp_ratio_180d,sp_ratio sp_ratio_180d,opp_sp_ratio,sp_ratio,`date` from dws.dws_pangge_shop_sp_day where `date`='${hivevar:day}'
