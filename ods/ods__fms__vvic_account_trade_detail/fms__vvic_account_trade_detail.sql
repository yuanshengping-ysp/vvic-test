create table if not exists `ods`.`fms__vvic_account_trade_detail` ( 
`trade_id` bigint comment '交易记录ID',
`user_id` bigint comment '用户ID',
`order_no` string comment '订单编号',
`trans_no` string comment '转账流水号',
`refund_no` string comment '退货编号',
`trade_type` int comment '交易类型; 1：充值，2：扣费，3：提现，4：退款',
`source_type` int comment '交易来源; 1.crm/ads系统 ,2.交易系统，3.fms系统，4.人工修正',
`trade_name` string comment '交易名称',
`trade_body` string comment '交易描述',
`account_id` bigint comment '用户登录ID',
`account_amount` double comment '用户交易后余额',
`gift_available_amount` double comment '用户交易后赠送余额',
`amount` double comment '交易金额',
`gift_amount` double comment '交易赠送金额',
`trade_msg` string comment '成功或失败原因',
`trade_st_time` string comment '交易起始时间',
`create_time` string comment '记录插入时间',
`update_time` string comment '记录更新时间',
`pay_time` string comment '付款时间',
`account_type` int comment '账户类型：1-CPC账户，2-CPT账户，3-实力质造账户' ) 
 stored as textfile 
 location 'hdfs://master:8020/user/hive/warehouse/ods.db/fms__vvic_account_trade_detail' ;