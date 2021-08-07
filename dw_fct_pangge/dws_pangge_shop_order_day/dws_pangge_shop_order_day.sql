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



---计算每天档口的入库量与入库数量(每天需要回溯过去30天的数据)减去7d
INSERT overwrite table dws.dws_pangge_shop_order_day partition(`date`='${hivevar:day}')
select t1.city_market_id,t1.shop_id
-- t1.pay_item_num
,count( distinct(
case when unix_timestamp(t2.receiving_scan_time, 'yyyy-MM-dd HH:mm:ss') <unix_timestamp(t1.pay_time, 'yyyy-MM-dd HH:mm:ss') +7*24*3600 then 
 t2.product_label_no 
 else null end )) receive_item_num
 ,count( distinct(
case when unix_timestamp(t2.receiving_scan_time, 'yyyy-MM-dd HH:mm:ss') <unix_timestamp(t1.pay_time, 'yyyy-MM-dd HH:mm:ss') +2*24*3600 then 
 t2.product_label_no 
 else null end )) receive_item_num_2d
 ,count( distinct(
case when unix_timestamp(t2.packing_time, 'yyyy-MM-dd HH:mm:ss') <unix_timestamp(t1.pay_time, 'yyyy-MM-dd HH:mm:ss') +2*24*3600 then 
 t2.product_label_no 
 else null end )) ship_item_num_2d
 from 
(select city_market_id,shop_id, order_details_id
,pay_item_num,pay_time
,order_no, `date` from dw.fct_pangge_pay_order_item
where  `date`='${hivevar:day}' ) t1
 join 
(select *  from ods.wms_product_label_merge
where  receiving_scan_flag=1 and not (qc_time is not null and product_label_status=9) 

) t2
on t1.order_details_id=cast(t2.ods_order_id as string) 
group by t1.city_market_id,t1.shop_id;





