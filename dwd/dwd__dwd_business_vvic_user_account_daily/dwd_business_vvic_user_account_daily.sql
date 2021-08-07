-- 步骤1：创建表
create table if not exists dwd.dwd_business_vvic_user_account_daily ( `id` bigint comment '流水号' ,`account_id` bigint comment '用户登录id' ,`user_id` bigint comment '用户id' ,`shop_id` bigint comment '档口id' ,`account_type` int comment '账户类型' ,`qc_amt` float comment '期初余额' ,`qm_amt` float comment '期末余额' ,`in_amt` float comment '转入金额' ,`out_amt` float comment '转出金额' ,`gift_qc_amt` float comment '赠送期初余额' ,`gift_qm_amt` float comment '赠送期末余额' ,`gift_out_amt` float comment '账户赠送转出金额' ,`gift_in_amt` float comment '账户赠送转入金额' ,`create_time` string comment '创建时间' ) comment '商业化账户每日汇总表' partitioned by (`date` string comment '统计日期' ) stored as parquet location 'hdfs://master:8020/user/hive/warehouse/dwd.db/dwd_business_vvic_user_account_daily';

--【获取记录并关联】
drop table if exists temp.dwd_business_vvic_user_account_daily_temp1 ;
create table if not exists temp.dwd_business_vvic_user_account_daily_temp1 as select t1.`id` ,t1.`account_id` ,t1.`user_id` ,t1.`account_type` ,t1.`begin_amount` as `qc_amt` ,t1.`end_amount` as `qm_amt` ,t1.`in_amount` as `in_amt` ,t1.`out_amount` as `out_amt` ,t1.`gift_out_amount` as `gift_out_amt` ,t1.`gift_in_amount` as `gift_in_amt` ,t1.`gift_begin_amount` as `gift_qc_amt` ,t1.`gift_end_amount` as `gift_qm_amt` ,t1.`create_time` ,t1.`trade_date` as `date` ,t2.`shop_id` from (select * from ods.fms__vvic_user_account_daily where `trade_date`>='${hivevar:day}' and `trade_date`<='${hivevar:day}') t1 left join ods.fms__vvic_user_account t2 on t1.`account_id`=t2.`account_id` ;

-- 结果数据写入
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.created.files=10000;
insert overwrite table dwd.dwd_business_vvic_user_account_daily partition(`date`) select `id` ,`account_id` ,`user_id` ,`shop_id` ,`account_type` ,`qc_amt` ,`qm_amt` ,`in_amt` ,`out_amt` ,`gift_qc_amt` ,`gift_qm_amt` ,`gift_out_amt` ,`gift_in_amt` ,`create_time` ,`date` from temp.dwd_business_vvic_user_account_daily_temp1 ;
