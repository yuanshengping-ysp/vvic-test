SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.rpt_pangge_shop_stock2_temp0 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp0 as select explode(split(CONCAT(date_sub('${hivevar:day}',2),',',date_sub('${hivevar:day}',1),',','${hivevar:day}'),',')) as stati_date ;


drop table if exists temp.rpt_pangge_shop_stock2_temp1 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp1 as select * from ods.wms__wms_product_label_log where create_time>=date_sub('${hivevar:day}',17) and create_time<date_add('${hivevar:day}',1) and current_life_status=0 ;

drop table if exists temp.rpt_pangge_shop_stock2_temp2 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp2 as select t1.* from (select * from ods.wms__wms_product_label_log where create_time>=date_sub('${hivevar:day}',17) ) t1 inner join temp.rpt_pangge_shop_stock2_temp1 t2 on t1.product_label_no=t2.product_label_no ;

drop table if exists temp.rpt_pangge_shop_stock2_temp3 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp3 as select product_label_no ,1 as cancel from temp.rpt_pangge_shop_stock2_temp2 where current_life_status=9 and origin_life_status=0 ;

drop table if exists temp.rpt_pangge_shop_stock2_temp4 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp4 as select product_label_no ,min(create_time) as receive_time from temp.rpt_pangge_shop_stock2_temp2 where current_life_status=4 group by product_label_no;

drop table if exists temp.rpt_pangge_shop_stock2_temp5 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp5 as select t1.* ,t2.shop_id ,t2.city_market_id ,t3.cancel ,t4.receive_time ,(case when t4.receive_time is not null then unix_timestamp(t4.receive_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(t1.create_time,'yyyy-MM-dd HH:mm:ss') else null end) as seconds from temp.rpt_pangge_shop_stock2_temp1 t1 left join (select * from dw.dim_wms_product_label where create_time>=date_sub('${hivevar:day}',17) and create_time<date_add('${hivevar:day}',1) ) t2 on t1.product_label_no=t2.product_label_no left join temp.rpt_pangge_shop_stock2_temp3 t3 on t1.product_label_no=t3.product_label_no left join temp.rpt_pangge_shop_stock2_temp4 t4 on t1.product_label_no=t4.product_label_no ;

drop table if exists temp.rpt_pangge_shop_stock2_temp6 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp6 as select t1.* ,t2.stati_date from temp.rpt_pangge_shop_stock2_temp5 t1 ,temp.rpt_pangge_shop_stock2_temp0 t2 where datediff(t2.stati_date,t1.create_time) between 0 and 13 ;

drop table if exists temp.rpt_pangge_shop_stock2_temp7 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp7 as select stati_date ,city_market_id ,shop_id ,count(1) as pay_item_num_2w ,count(case when cancel is null then 1 else null end) as enallot_item_num_2w ,count(case when seconds<=172800 and seconds>=0 then 1 else null end) as receive_item_num_2d from temp.rpt_pangge_shop_stock2_temp6 group by stati_date ,city_market_id ,shop_id  ;

drop table if exists temp.rpt_pangge_shop_stock2_temp8 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp8 as select stati_date ,city_market_id ,count(case when pay_item_num_2w>0 then 1 else null end) as pay_shop_num_2w ,count(case when pay_item_num_2w>=45 then 1 else null end) as pay_shop_num_2w_45 from temp.rpt_pangge_shop_stock2_temp7 group by stati_date ,city_market_id ;

drop table if exists temp.rpt_pangge_shop_stock2_temp9 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp9 as select t1.stati_date as `date` ,t1.city_market_id ,t1.shop_id ,t2.pay_shop_num_2w ,t2.pay_shop_num_2w_45 ,t1.pay_item_num_2w ,t1.enallot_item_num_2w ,t1.receive_item_num_2d ,t1.receive_item_num_2d*1.0/t1.enallot_item_num_2w as receive_item_num_ratio_2d from temp.rpt_pangge_shop_stock2_temp7 t1 left join temp.rpt_pangge_shop_stock2_temp8 t2 on t1.stati_date=t2.stati_date and t1.city_market_id=t2.city_market_id ;

drop table if exists temp.rpt_pangge_shop_stock2_temp10 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp10 as select `date` ,shop_id ,city_market_id ,pay_item_num_2w ,receive_item_num_ratio_2d ,rank() over (partition by `date`,city_market_id order by receive_item_num_ratio_2d desc) as receive_item_num_ratio_2d_rank from temp.rpt_pangge_shop_stock2_temp9 where pay_item_num_2w>0 ;

drop table if exists temp.rpt_pangge_shop_stock2_temp11 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp11 as select `date` ,shop_id ,city_market_id ,pay_item_num_2w ,receive_item_num_ratio_2d ,rank() over (partition by `date`,city_market_id order by receive_item_num_ratio_2d desc) as receive_item_num_ratio_2d_rank_45 from temp.rpt_pangge_shop_stock2_temp9 where pay_item_num_2w>=45 ;

drop table if exists temp.rpt_pangge_shop_stock2_temp12 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp12 as select t1.* ,t2.receive_item_num_ratio_2d_rank ,t3.receive_item_num_ratio_2d_rank_45 from temp.rpt_pangge_shop_stock2_temp9 t1 left join temp.rpt_pangge_shop_stock2_temp10 t2 on t1.`date`=t2.`date` and t1.shop_id=t2.shop_id and t1.city_market_id=t2.city_market_id left join temp.rpt_pangge_shop_stock2_temp11 t3 on t1.`date`=t3.`date` and t1.shop_id=t3.shop_id and t1.city_market_id=t3.city_market_id ;

drop table if exists temp.rpt_pangge_shop_stock2_temp13 ;
create table if not exists temp.rpt_pangge_shop_stock2_temp13 as select * ,(case when receive_item_num_ratio_2d_rank>0 then 1-receive_item_num_ratio_2d_rank*1.0/pay_shop_num_2w else null end) as receive_item_num_ratio_2d_compare ,(case when receive_item_num_ratio_2d_rank_45>0 then 1-receive_item_num_ratio_2d_rank_45*1.0/pay_shop_num_2w_45 else null end) as receive_item_num_ratio_2d_compare_45 from temp.rpt_pangge_shop_stock2_temp12 ;


insert overwrite table rpt.rpt_pangge_shop_stock2 partition(`date`) select city_market_id ,shop_id ,pay_shop_num_2w ,pay_shop_num_2w_45 ,pay_item_num_2w ,enallot_item_num_2w ,receive_item_num_2d ,receive_item_num_ratio_2d ,receive_item_num_ratio_2d_rank ,receive_item_num_ratio_2d_compare ,receive_item_num_ratio_2d_rank_45 ,receive_item_num_ratio_2d_compare_45 ,`date` from temp.rpt_pangge_shop_stock2_temp13  ;

