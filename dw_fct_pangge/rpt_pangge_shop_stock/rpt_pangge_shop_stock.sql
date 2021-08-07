SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.rpt_pangge_shop_stock_temp0 ;
create table if not exists temp.rpt_pangge_shop_stock_temp0 as select explode(split(CONCAT(date_sub('${hivevar:day}',2),',',date_sub('${hivevar:day}',1),',','${hivevar:day}'),',')) as stati_date ;

drop table if exists temp.rpt_pangge_shop_stock_temp1 ;
create table if not exists temp.rpt_pangge_shop_stock_temp1 as select `date` ,city_market_id ,shop_id ,count(1) as pay_item_num from dw.dim_wms_product_label where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}' and `date`>='2019-12-17' and label_type in (0,1) and order_type = 0 group by `date` ,city_market_id ,shop_id ;

drop table if exists temp.rpt_pangge_shop_stock_temp2 ;
create table if not exists temp.rpt_pangge_shop_stock_temp2 as select t1.* ,t2.stati_date from temp.rpt_pangge_shop_stock_temp1 t1 ,temp.rpt_pangge_shop_stock_temp0 t2 ;

drop table if exists temp.rpt_pangge_shop_stock_temp3 ;
create table if not exists temp.rpt_pangge_shop_stock_temp3 as select stati_date ,city_market_id ,shop_id ,sum(case when datediff(stati_date,`date`) between 0 and 13 then pay_item_num else 0 end) as pay_item_num_2w from temp.rpt_pangge_shop_stock_temp2 group by stati_date ,city_market_id ,shop_id ;

drop table if exists temp.rpt_pangge_shop_stock_temp4 ;
create table if not exists temp.rpt_pangge_shop_stock_temp4 as select `date` ,city_market_id ,shop_id ,sum(allot_item_num) as allot_item_num ,sum(receive_item_num_2d) as receive_item_num_2d from dw.fct_pangge_stock_item where `date`>=date_sub('${hivevar:day}',16) and `date`<='${hivevar:day}' group by `date` ,city_market_id ,shop_id ;

drop table if exists temp.rpt_pangge_shop_stock_temp5 ;
create table if not exists temp.rpt_pangge_shop_stock_temp5 as select t1.* ,t2.stati_date from temp.rpt_pangge_shop_stock_temp4 t1 ,temp.rpt_pangge_shop_stock_temp0 t2 ;

drop table if exists temp.rpt_pangge_shop_stock_temp6 ;
create table if not exists temp.rpt_pangge_shop_stock_temp6 as select stati_date ,city_market_id ,shop_id ,sum(case when datediff(stati_date,`date`) between 0 and 13 then allot_item_num else 0 end) as allot_item_num_2w ,sum(case when datediff(stati_date,`date`) between 0 and 13 then receive_item_num_2d else 0 end) as receive_item_num_2d from temp.rpt_pangge_shop_stock_temp5 group by stati_date ,city_market_id ,shop_id  ;

drop table if exists temp.rpt_pangge_shop_stock_temp7 ;
create table if not exists temp.rpt_pangge_shop_stock_temp7 as select stati_date ,city_market_id ,count(case when pay_item_num_2w>0 then 1 else null end) as pay_shop_num_2w ,count(case when pay_item_num_2w>=45 then 1 else null end) as pay_shop_num_2w_45 from temp.rpt_pangge_shop_stock_temp3 group by stati_date ,city_market_id ;

drop table if exists temp.rpt_pangge_shop_stock_temp8 ;
create table if not exists temp.rpt_pangge_shop_stock_temp8 as select stati_date ,count(distinct `date`) as days from temp.rpt_pangge_shop_stock_temp5 group by stati_date ;

drop table if exists temp.rpt_pangge_shop_stock_temp9 ;
create table if not exists temp.rpt_pangge_shop_stock_temp9 as select t1.stati_date as `date` ,t1.city_market_id ,t1.shop_id ,t3.pay_shop_num_2w ,t3.pay_shop_num_2w_45 ,t1.pay_item_num_2w ,t2.allot_item_num_2w ,t2.receive_item_num_2d ,t2.receive_item_num_2d*1.0/t2.allot_item_num_2w as receive_item_num_ratio_2d ,t4.days from temp.rpt_pangge_shop_stock_temp3 t1 left join temp.rpt_pangge_shop_stock_temp6 t2 on t1.stati_date=t2.stati_date and t1.shop_id=t2.shop_id left join temp.rpt_pangge_shop_stock_temp7 t3 on t1.stati_date=t3.stati_date and t1.city_market_id=t3.city_market_id left join temp.rpt_pangge_shop_stock_temp8 t4 on t1.stati_date=t4.stati_date ;

drop table if exists temp.rpt_pangge_shop_stock_temp10 ;
create table if not exists temp.rpt_pangge_shop_stock_temp10 as select `date` ,shop_id ,city_market_id ,pay_item_num_2w ,receive_item_num_ratio_2d ,rank() over (partition by `date`,city_market_id order by receive_item_num_ratio_2d desc) as receive_item_num_ratio_2d_rank from temp.rpt_pangge_shop_stock_temp9 where pay_item_num_2w>0 ;

drop table if exists temp.rpt_pangge_shop_stock_temp11 ;
create table if not exists temp.rpt_pangge_shop_stock_temp11 as select `date` ,shop_id ,city_market_id ,pay_item_num_2w ,receive_item_num_ratio_2d ,rank() over (partition by `date`,city_market_id order by receive_item_num_ratio_2d desc) as receive_item_num_ratio_2d_rank_45 from temp.rpt_pangge_shop_stock_temp9 where pay_item_num_2w>=45 ;

drop table if exists temp.rpt_pangge_shop_stock_temp12 ;
create table if not exists temp.rpt_pangge_shop_stock_temp12 as select t1.* ,t2.receive_item_num_ratio_2d_rank ,t3.receive_item_num_ratio_2d_rank_45 from temp.rpt_pangge_shop_stock_temp9 t1 left join temp.rpt_pangge_shop_stock_temp10 t2 on t1.`date`=t2.`date` and t1.shop_id=t2.shop_id and t1.city_market_id=t2.city_market_id left join temp.rpt_pangge_shop_stock_temp11 t3 on t1.`date`=t3.`date` and t1.shop_id=t3.shop_id and t1.city_market_id=t3.city_market_id ;

drop table if exists temp.rpt_pangge_shop_stock_temp13 ;
create table if not exists temp.rpt_pangge_shop_stock_temp13 as select * ,(case when receive_item_num_ratio_2d_rank>0 then 1-receive_item_num_ratio_2d_rank*1.0/pay_shop_num_2w else null end) as receive_item_num_ratio_2d_compare ,(case when receive_item_num_ratio_2d_rank_45>0 then 1-receive_item_num_ratio_2d_rank_45*1.0/pay_shop_num_2w_45 else null end) as receive_item_num_ratio_2d_compare_45 from temp.rpt_pangge_shop_stock_temp12 where days>=0 ;


insert overwrite table rpt.rpt_pangge_shop_stock partition(`date`) select city_market_id ,shop_id ,pay_shop_num_2w ,pay_shop_num_2w_45 ,pay_item_num_2w ,allot_item_num_2w ,receive_item_num_2d ,receive_item_num_ratio_2d ,receive_item_num_ratio_2d_rank ,receive_item_num_ratio_2d_compare ,receive_item_num_ratio_2d_rank_45 ,receive_item_num_ratio_2d_compare_45 ,`date` from temp.rpt_pangge_shop_stock_temp13 where `date`>=date_sub('${hivevar:day}',2) and `date`<='${hivevar:day}' ;

