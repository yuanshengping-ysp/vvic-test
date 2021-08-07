SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.dwd_pangge_allot_label_temp0 ;
create table if not exists temp.dwd_pangge_allot_label_temp0 as select '${hivevar:day}' as `stati_date` ,concat_ws(' ', cast('${hivevar:day}' as string),'15:00:00') as `stati_end_time` ,concat_ws(' ', cast(date_sub('${hivevar:day}',1) as string),'15:00:00') as `stati_begin_time` ;

drop table if exists temp.dwd_pangge_allot_label_temp1 ;
create table if not exists temp.dwd_pangge_allot_label_temp1 as select `id` ,`product_label_no` ,`current_life_status` ,`create_time` ,2 as `source` from ods.wms__wms_product_label_log where `create_time`>=concat_ws(' ', cast(date_sub('${hivevar:day}',1) as string),'15:00:00') and `current_life_status`=4 union all select cast(null as bigint) as `id` ,`product_label_no` ,4 as `current_life_status` ,`receiving_scan_time` as `create_time` ,1 as `source` from ods.wms_product_label where `receiving_scan_time`>=concat_ws(' ', cast(date_sub('${hivevar:day}',1) as string),'15:00:00') ;

drop table if exists temp.dwd_pangge_allot_label_temp2 ;
create table if not exists temp.dwd_pangge_allot_label_temp2 as select t1.* ,t2.`create_time` from temp.dwd_pangge_allot_label_temp0 t1 left join ods.wms__t_store_operation t2 on t1.`stati_date`=t2.`ymd` ;

drop table if exists temp.dwd_pangge_allot_label_temp3 ;
create table if not exists temp.dwd_pangge_allot_label_temp3 as select t2.`stati_date` as `date` ,t1.`id` ,t1.`create_time` ,t1.`product_label_no` ,t1.`current_life_status` ,0 as `method` ,t1.`source` from temp.dwd_pangge_allot_label_temp1 t1 ,temp.dwd_pangge_allot_label_temp2 t2 where t1.`create_time`>=t2.`stati_begin_time` and t1.`create_time`<=t2.`create_time` ;

drop table if exists temp.dwd_pangge_allot_label_temp4 ;
create table if not exists temp.dwd_pangge_allot_label_temp4 as select t1.* from (select * ,lead(`create_time`,1) over(partition by `date`,`product_label_no` order by `source`,`create_time`) as `next_create_time` from temp.dwd_pangge_allot_label_temp3) t1 where t1.`next_create_time` is null ;

drop table if exists temp.dwd_pangge_allot_label_temp5 ;
create table if not exists temp.dwd_pangge_allot_label_temp5 as select t1.* from temp.dwd_pangge_allot_label_temp4 t1 inner join (select * from dwd.dwd_pangge_print_label where `date`>='${hivevar:day}' and `date`<='${hivevar:day}') t2 on t1.`date`=t2.`date` and t1.`product_label_no`=t2.`product_label_no` ;

drop table if exists temp.dwd_pangge_allot_label_temp6 ;
create table if not exists temp.dwd_pangge_allot_label_temp6 as select t1.* from temp.dwd_pangge_allot_label_temp5 t1 left join dw.dim_wms_product_label t2 on t1.`product_label_no`=t2.`product_label_no` where t2.order_type=0 ;

insert overwrite table dwd.dwd_pangge_allot_label partition(`date`='${hivevar:day}') select `id` ,`product_label_no` ,`current_life_status` ,`create_time` ,`method` from temp.dwd_pangge_allot_label_temp6 where `date`='${hivevar:day}' ;

