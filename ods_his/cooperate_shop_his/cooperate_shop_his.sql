SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.fct_ods_table_vvic__cooperate_shop_temp0 ;

create table if not exists temp.fct_ods_table_vvic__cooperate_shop_temp0 as select `id` as `id`,`shop_id` as `shop_id`,`lower_price` as `lower_price`,`create_time` as `create_time`,`create_user` as `create_user`,`update_time` as `update_time`,`update_user` as `update_user`,`status` as `status`,`effective_ymd` as `effective_ymd`,`invalid_time` as `invalid_time` ,1 as is_valid ,date_add('${hivevar:day}',1) as create_date ,date_add('${hivevar:day}',1) as update_date from ods.vvic__cooperate_shop ;

drop table if exists temp.fct_ods_table_vvic__cooperate_shop_temp1 ;

create table if not exists temp.fct_ods_table_vvic__cooperate_shop_temp1 as select distinct t3.`id`,t3.`shop_id`,t3.`lower_price`,t3.`create_time`,t3.`create_user`,t3.`update_time`,t3.`update_user`,t3.`status`,t3.`effective_ymd`,t3.`invalid_time` from (select t1.`id`,t1.`shop_id`,t1.`lower_price`,t1.`create_time`,t1.`create_user`,t1.`update_time`,t1.`update_user`,t1.`status`,t1.`effective_ymd`,t1.`invalid_time`  from temp.fct_ods_table_vvic__cooperate_shop_temp0 t1 union all select t2.`id`,t2.`shop_id`,t2.`lower_price`,t2.`create_time`,t2.`create_user`,t2.`update_time`,t2.`update_user`,t2.`status`,t2.`effective_ymd`,t2.`invalid_time`  from ods.vvic__cooperate_shop_his t2 where t2.is_valid=1) t3 ;

drop table if exists temp.fct_ods_table_vvic__cooperate_shop_temp2 ;

create table if not exists temp.fct_ods_table_vvic__cooperate_shop_temp2 as select t1.id ,count(1) as num from temp.fct_ods_table_vvic__cooperate_shop_temp1 t1 group by t1.id;

drop table if exists temp.fct_ods_table_vvic__cooperate_shop_temp3 ;

create table if not exists temp.fct_ods_table_vvic__cooperate_shop_temp3 as select t2.`id`,t2.`shop_id`,t2.`lower_price`,t2.`create_time`,t2.`create_user`,t2.`update_time`,t2.`update_user`,t2.`status`,t2.`effective_ymd`,t2.`invalid_time` ,1 as is_valid ,(case when t3.create_date is null then date_add('${hivevar:day}',1) else t3.create_date end) as create_date ,date_add('${hivevar:day}',1) as update_date from (select * from temp.fct_ods_table_vvic__cooperate_shop_temp2 where num=1 and id is not null) t1 left join (select * from temp.fct_ods_table_vvic__cooperate_shop_temp1) t2 on t1.`id`=t2.`id` left join (select * from ods.vvic__cooperate_shop_his where is_valid=1) t3 on t1.`id`=t3.`id` ;

drop table if exists temp.fct_ods_table_vvic__cooperate_shop_temp4 ;

create table if not exists temp.fct_ods_table_vvic__cooperate_shop_temp4 as select t2.`id`,t2.`shop_id`,t2.`lower_price`,t2.`create_time`,t2.`create_user`,t2.`update_time`,t2.`update_user`,t2.`status`,t2.`effective_ymd`,t2.`invalid_time` ,0 as is_valid ,t2.create_date ,date_add('${hivevar:day}',1) as update_date from (select * from temp.fct_ods_table_vvic__cooperate_shop_temp2 where num=2) t1 left join (select * from ods.vvic__cooperate_shop_his where is_valid=1) t2 on t1.`id`=t2.`id` ;

drop table if exists temp.fct_ods_table_vvic__cooperate_shop_temp5 ;

create table if not exists temp.fct_ods_table_vvic__cooperate_shop_temp5 as select t2.`id`,t2.`shop_id`,t2.`lower_price`,t2.`create_time`,t2.`create_user`,t2.`update_time`,t2.`update_user`,t2.`status`,t2.`effective_ymd`,t2.`invalid_time` ,1 as is_valid ,t2.create_date ,date_add('${hivevar:day}',1) as update_date from (select * from temp.fct_ods_table_vvic__cooperate_shop_temp2 where num=2) t1 left join (select * from temp.fct_ods_table_vvic__cooperate_shop_temp0) t2 on t1.`id`=t2.`id` ;

drop table if exists temp.fct_ods_table_vvic__cooperate_shop_temp6 ;

create table if not exists temp.fct_ods_table_vvic__cooperate_shop_temp6 as select * from ods.vvic__cooperate_shop_his where is_valid=0 and `id` is not null;

insert overwrite table ods.vvic__cooperate_shop_his select `id`,`shop_id`,`lower_price`,`create_time`,`create_user`,`update_time`,`update_user`,`status`,`effective_ymd`,`invalid_time` ,is_valid ,cast(create_date as date) create_date ,cast(update_date as date) update_date from temp.fct_ods_table_vvic__cooperate_shop_temp3 union all select `id`,`shop_id`,`lower_price`,`create_time`,`create_user`,`update_time`,`update_user`,`status`,`effective_ymd`,`invalid_time` ,is_valid ,cast(create_date as date) create_date ,cast(update_date as date) update_date from temp.fct_ods_table_vvic__cooperate_shop_temp4 union all select `id`,`shop_id`,`lower_price`,`create_time`,`create_user`,`update_time`,`update_user`,`status`,`effective_ymd`,`invalid_time` ,is_valid ,cast(create_date as date) create_date,cast(update_date as date) update_date from temp.fct_ods_table_vvic__cooperate_shop_temp5 union all select `id`,`shop_id`,`lower_price`,`create_time`,`create_user`,`update_time`,`update_user`,`status`,`effective_ymd`,`invalid_time` ,is_valid ,cast(create_date as date) create_date ,cast(update_date as date) update_date from temp.fct_ods_table_vvic__cooperate_shop_temp6 ;
