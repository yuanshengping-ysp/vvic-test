create table if not exists `ods`.`vvic__t_member_shop_order` ( 
`order_id` bigint comment '订单ID',
`vas_order_id` bigint comment '交易订单ID',
`pay_no` bigint comment '支付订单号',
`user_id` bigint comment '用户ID',
`shop_id` int comment '档口ID',
`city_market_id` bigint comment '站点ID',
`biz_type` int comment '业务类型',
`order_status` int comment '订单状态',
`vas_id` bigint comment '增值服务产品ID',
`product_name` string comment '产品名称',
`product_type` int comment '增值服务产品类型',
`vas_sku_id` bigint comment '增值服务产品-SKUID',
`cycle_name` string comment '售卖周期名称',
`cycle_val` int comment '售卖周期值',
`cycle_type` int comment '售卖周期值类型',
`cycle_start` string comment '生效时间',
`cycle_end` string comment '失效时间',
`total_pay_fee` double comment '订单优惠',
`pay_type` int comment '支付类型',
`create_user` bigint comment '创建用户ID',
`update_user` bigint comment '更新用户ID',
`create_time` string comment '创建时间',
`update_time` string comment '更新时间',
`cancel_time` string comment '取消时间',
`cancel_reason` string comment '取消原因',
`pay_time` string comment '支付时间',
`real_cycle_end` string comment '实际失效时间',
`termination_user` bigint comment '终止操作人ID',
`termination_username` string comment '终止操作人姓名',
`termination_reason` string comment '终止原因',
`owner_user_id` bigint comment '档口负责人',
`owner_user_name` string comment '档口负责人姓名' ) 
 stored as textfile 
 location 'hdfs://master:8020/user/hive/warehouse/ods.db/vvic__t_member_shop_order' ;