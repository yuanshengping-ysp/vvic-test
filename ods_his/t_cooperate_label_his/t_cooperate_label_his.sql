SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.fct_ods_table_wms__t_cooperate_label_temp0 ;
create table if not exists temp.fct_ods_table_wms__t_cooperate_label_temp0 as select `id` as `id`,`product_label_id` as `product_label_id`,`product_label_no` as `product_label_no`,`oper_id` as `oper_id`,`oper_name` as `oper_name`,`oper_id_his` as `oper_id_his`,`ymd` as `ymd`,`create_time` as `create_time`,`update_time` as `update_time` ,1 as is_valid ,date_add('${hivevar:day}',1) as create_date ,date_add('${hivevar:day}',1) as update_date from ods.wms__t_cooperate_label ;

drop table if exists temp.fct_ods_table_wms__t_cooperate_label_temp1 ;
create table if not exists temp.fct_ods_table_wms__t_cooperate_label_temp1 as select distinct t3.`id`,t3.`product_label_id`,t3.`product_label_no`,t3.`oper_id`,t3.`oper_name`,t3.`oper_id_his`,t3.`ymd`,t3.`create_time`,t3.`update_time` from (select t1.`id`,t1.`product_label_id`,t1.`product_label_no`,t1.`oper_id`,t1.`oper_name`,t1.`oper_id_his`,t1.`ymd`,t1.`create_time`,t1.`update_time`  from temp.fct_ods_table_wms__t_cooperate_label_temp0 t1 union all select t2.`id`,t2.`product_label_id`,t2.`product_label_no`,t2.`oper_id`,t2.`oper_name`,t2.`oper_id_his`,t2.`ymd`,t2.`create_time`,t2.`update_time`  from ods.wms__t_cooperate_label_his t2 where t2.is_valid=1) t3 ;

drop table if exists temp.fct_ods_table_wms__t_cooperate_label_temp2 ;
create table if not exists temp.fct_ods_table_wms__t_cooperate_label_temp2 as select t1.id ,count(1) as num from temp.fct_ods_table_wms__t_cooperate_label_temp1 t1 group by t1.id;

drop table if exists temp.fct_ods_table_wms__t_cooperate_label_temp3 ;
create table if not exists temp.fct_ods_table_wms__t_cooperate_label_temp3 as select t2.`id`,t2.`product_label_id`,t2.`product_label_no`,t2.`oper_id`,t2.`oper_name`,t2.`oper_id_his`,t2.`ymd`,t2.`create_time`,t2.`update_time` ,1 as is_valid ,(case when t3.create_date is null then date_add('${hivevar:day}',1) else t3.create_date end) as create_date ,date_add('${hivevar:day}',1) as update_date from (select * from temp.fct_ods_table_wms__t_cooperate_label_temp2 where num=1 and id is not null) t1 left join (select * from temp.fct_ods_table_wms__t_cooperate_label_temp1) t2 on t1.`id`=t2.`id` left join (select * from ods.wms__t_cooperate_label_his where is_valid=1) t3 on t1.`id`=t3.`id` ;

drop table if exists temp.fct_ods_table_wms__t_cooperate_label_temp4 ;
create table if not exists temp.fct_ods_table_wms__t_cooperate_label_temp4 as select t2.`id`,t2.`product_label_id`,t2.`product_label_no`,t2.`oper_id`,t2.`oper_name`,t2.`oper_id_his`,t2.`ymd`,t2.`create_time`,t2.`update_time` ,0 as is_valid ,t2.create_date ,date_add('${hivevar:day}',1) as update_date from (select * from temp.fct_ods_table_wms__t_cooperate_label_temp2 where num=2) t1 left join (select * from ods.wms__t_cooperate_label_his where is_valid=1) t2 on t1.`id`=t2.`id` ;

drop table if exists temp.fct_ods_table_wms__t_cooperate_label_temp5 ;
create table if not exists temp.fct_ods_table_wms__t_cooperate_label_temp5 as select t2.`id`,t2.`product_label_id`,t2.`product_label_no`,t2.`oper_id`,t2.`oper_name`,t2.`oper_id_his`,t2.`ymd`,t2.`create_time`,t2.`update_time` ,1 as is_valid ,t2.create_date ,date_add('${hivevar:day}',1) as update_date from (select * from temp.fct_ods_table_wms__t_cooperate_label_temp2 where num=2) t1 left join (select * from temp.fct_ods_table_wms__t_cooperate_label_temp0) t2 on t1.`id`=t2.`id` ;

drop table if exists temp.fct_ods_table_wms__t_cooperate_label_temp6 ;
create table if not exists temp.fct_ods_table_wms__t_cooperate_label_temp6 as select * from ods.wms__t_cooperate_label_his where is_valid=0 and `id` is not null;

insert overwrite table ods.wms__t_cooperate_label_his select `id`,`product_label_id`,`product_label_no`,`oper_id`,`oper_name`,`oper_id_his`,`ymd`,`create_time`,`update_time` ,is_valid ,cast(create_date as date) create_date ,cast(update_date as date) update_date from temp.fct_ods_table_wms__t_cooperate_label_temp3 union all select `id`,`product_label_id`,`product_label_no`,`oper_id`,`oper_name`,`oper_id_his`,`ymd`,`create_time`,`update_time` ,is_valid ,cast(create_date as date) create_date ,cast(update_date as date) update_date from temp.fct_ods_table_wms__t_cooperate_label_temp4 union all select `id`,`product_label_id`,`product_label_no`,`oper_id`,`oper_name`,`oper_id_his`,`ymd`,`create_time`,`update_time` ,is_valid ,cast(create_date as date) create_date ,cast(update_date as date) update_date from temp.fct_ods_table_wms__t_cooperate_label_temp5 union all select `id`,`product_label_id`,`product_label_no`,`oper_id`,`oper_name`,`oper_id_his`,`ymd`,`create_time`,`update_time` ,is_valid ,cast(create_date as date) create_date,cast(update_date as date) update_date from temp.fct_ods_table_wms__t_cooperate_label_temp6 ;
