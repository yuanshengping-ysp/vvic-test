SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000 ;
set hive.exec.max.dynamic.partitions=1000 ;
set hive.exec.max.created.files=1000 ;

drop table if exists temp.fct_cooperate_return_label_temp0 ;
create table if not exists temp.fct_cooperate_return_label_temp0 as select explode(split(CONCAT(date_sub('${hivevar:day}',1),',','${hivevar:day}'),',')) as stati_date ;

drop table if exists temp.fct_cooperate_return_label_temp1 ;
create table if not exists temp.fct_cooperate_return_label_temp1 as select substring(t1.return_ack_time,1,10) as `date` ,t1.return_ack_time as return_time ,t1.return_batch_id ,t1.shop_id ,t2.sku_id as item_sku_id ,t2.num as return_item_num ,t2.success_num as return_success_item_num ,unix_timestamp(t1.complete_time)-unix_timestamp(t1.return_ack_time) as return_seconds from (select * from ods.pms__t_return_sku_batch where return_ack_time>=date_sub('${hivevar:day}',1) and return_ack_time<'${hivevar:day}' ) t1 left join ods.pms__t_return_sku_detail t2 on t1.return_batch_id=t2.return_batch_id ;

drop table if exists temp.fct_cooperate_return_label_temp2 ;
create table if not exists temp.fct_cooperate_return_label_temp2 as select t1.* from temp.fct_cooperate_return_label_temp1 t1 inner join temp.fct_cooperate_return_label_temp0 t2 on t1.`date`=t2.stati_date ;


insert overwrite table dw.fct_cooperate_return_label partition(`date`) select return_time ,return_batch_id ,shop_id ,item_sku_id ,return_item_num ,return_success_item_num ,return_seconds ,`date` from temp.fct_cooperate_return_label_temp2;

