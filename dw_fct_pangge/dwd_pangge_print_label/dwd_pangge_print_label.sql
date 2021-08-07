SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;



drop table if exists temp.dwd_pangge_print_label_temp0 ;
create table if not exists temp.dwd_pangge_print_label_temp0 as select '${hivevar:day}' as `stati_date` ,concat_ws(' ', cast('${hivevar:day}' as string),'15:00:00') as `stati_end_time` ,concat_ws(' ', cast(date_sub('${hivevar:day}',1) as string),'15:00:00') as `stati_begin_time` ;

drop table if exists temp.dwd_pangge_print_label_temp1 ;
create table if not exists temp.dwd_pangge_print_label_temp1 as select t2.`stati_date` as `date` ,t1.`id` ,t1.`product_label_no` ,t1.`current_life_status` ,t1.`create_time` ,0 as `method` from (select * from ods.wms__wms_product_label_log where `create_time`>=concat_ws(' ', cast(date_sub('${hivevar:day}',1) as string),'15:00:00') and `create_time`<concat_ws(' ', cast('${hivevar:day}' as string),'15:00:00') and `current_life_status` in (1)) t1,temp.dwd_pangge_print_label_temp0 t2 where t1.`create_time`>=t2.`stati_begin_time` and t1.`create_time`<t2.`stati_end_time` ;

drop table if exists temp.dwd_pangge_print_label_temp2 ;
create table if not exists temp.dwd_pangge_print_label_temp2 as select t1.* from (select * ,lead(`create_time`,1) over(partition by `date`,`product_label_no` order by `create_time`) as `next_create_time` from temp.dwd_pangge_print_label_temp1) t1 where t1.`next_create_time` is null ;

drop table if exists temp.dwd_pangge_print_label_temp3 ;
create table if not exists temp.dwd_pangge_print_label_temp3 as select t1.* from temp.dwd_pangge_print_label_temp2 t1 left join dw.dim_wms_product_label t2 on t1.`product_label_no`=t2.`product_label_no` where t2.order_type=0 ;

insert overwrite table dwd.dwd_pangge_print_label partition(`date`='${hivevar:day}') select `id` ,`product_label_no` ,`current_life_status` ,`create_time` ,`method` from temp.dwd_pangge_print_label_temp3 where `date`='${hivevar:day}' ;

