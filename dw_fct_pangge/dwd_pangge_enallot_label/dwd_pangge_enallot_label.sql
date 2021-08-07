SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.dwd_pangge_enallot_label_temp0 ;
create table if not exists temp.dwd_pangge_enallot_label_temp0 as 
select '${hivevar:day}' as `stati_date` ,concat_ws(' ', cast('${hivevar:day}' as string),'15:00:00') as `stati_end_time` ,concat_ws(' ', cast(date_sub('${hivevar:day}',1) as string),'15:00:00') as `stati_begin_time` ;

drop table if exists temp.dwd_pangge_enallot_label_temp1 ;
create table if not exists temp.dwd_pangge_enallot_label_temp1 as select t3.`stati_date` as `date` ,t3.`product_label_no` ,t3.`id` ,t3.`create_time` ,t3.`current_life_status` ,0 as `method` from (select t2.* ,t1.* ,lead(`create_time`,1) over(partition by t2.`stati_date`,t1.`product_label_no` order by t1.`create_time`) as `next_create_time` from (select * from ods.wms__wms_product_label_log where create_time<concat_ws(' ', cast('${hivevar:day}' as string),'15:00:00')) t1 ,temp.dwd_pangge_enallot_label_temp0 t2 where t1.`create_time`<t2.`stati_end_time`) t3 where t3.current_life_status in (0,3) and t3.`next_create_time` is null ;

drop table if exists temp.dwd_pangge_enallot_label_temp2 ;
create table if not exists temp.dwd_pangge_enallot_label_temp2 as select t1.* from temp.dwd_pangge_enallot_label_temp1 t1 left join (select * from dwd.dwd_pangge_print_label where `date`>='${hivevar:day}' and `date`<='${hivevar:day}') t2 on t1.`date`=t2.`date` and t1.`product_label_no`=t2.`product_label_no` where t2.`date` is null ;

drop table if exists temp.dwd_pangge_enallot_label_temp3 ;
create table if not exists temp.dwd_pangge_enallot_label_temp3 as select t1.* from temp.dwd_pangge_enallot_label_temp2 t1 left join dw.dim_wms_product_label t2 on t1.`product_label_no`=t2.`product_label_no` where t2.order_type=0 ;

insert overwrite table dwd.dwd_pangge_enallot_label partition(`date`='${hivevar:day}') select `id` ,`product_label_no` ,`current_life_status` ,`create_time` ,`method` from temp.dwd_pangge_enallot_label_temp3 where `date`='${hivevar:day}' ;
