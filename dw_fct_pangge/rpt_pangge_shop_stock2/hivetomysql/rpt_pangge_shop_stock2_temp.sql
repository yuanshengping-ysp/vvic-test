SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.rpt_pangge_shop_stock2 ;

create table temp.rpt_pangge_shop_stock2 as
select city_market_id,shop_id,pay_shop_num_2w,pay_shop_num_2w_45,pay_item_num_2w,enallot_item_num_2w allot_item_num_2w,receive_item_num_2d,receive_item_num_ratio_2d,receive_item_num_ratio_2d_rank,receive_item_num_ratio_2d_compare,receive_item_num_ratio_2d_rank_45,receive_item_num_ratio_2d_compare_45,`date` from rpt.rpt_pangge_shop_stock2 where `date`='${hivevar:day}'
