create table if not exists `ods`.`fms__vvic_user_account_daily` ( 
`id` bigint comment 'ID',
`account_id` bigint comment '用户ID',
`user_id` bigint comment '用户ID',
`account_type` int comment '账户类型 1.cpc广告账户,2.cpt广告账户,3.用户余额账户',
`begin_amount` double comment '开始时点余额',
`end_amount` double comment '结束时点余额',
`in_amount` double comment '转入金额',
`out_amount` double comment '转出金额',
`gift_out_amount` double comment '账户赠送支出金额',
`gift_in_amount` double comment '账户赠送入金额',
`gift_end_amount` double comment '账户赠送期末金额',
`gift_begin_amount` double comment '账户赠送期初金额',
`create_time` string comment '记录添加时间',
`trade_date` string comment '交易时间' ) 
 stored as textfile 
 location 'hdfs://master:8020/user/hive/warehouse/ods.db/fms__vvic_user_account_daily' ;