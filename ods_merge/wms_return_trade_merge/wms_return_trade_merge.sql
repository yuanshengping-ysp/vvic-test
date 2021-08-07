SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.wms_return_trade_merge_temp0 ;
create table if not exists temp.wms_return_trade_merge_temp0 as select return_trade_id ,return_trade_no ,send_trade_no ,ods_return_trade_id ,return_type ,return_express_no ,return_express_id ,return_express_code ,return_express_company ,return_address ,return_phone ,return_reason ,return_reason_type ,return_remark ,return_payment ,return_pay_time ,return_trade_status ,buyer_id ,buyer_nick ,receiver_name ,reveiver_time ,remove_flag ,create_time ,creator ,update_time ,mender ,usable_flag ,city_market_id ,1 as table_type from ods.wms__wms_return_trade union all select return_trade_id ,return_trade_no ,send_trade_no ,ods_return_trade_id ,return_type ,return_express_no ,return_express_id ,return_express_code ,return_express_company ,return_address ,return_phone ,return_reason ,return_reason_type ,return_remark ,return_payment ,return_pay_time ,return_trade_status ,buyer_id ,buyer_nick ,receiver_name ,reveiver_time ,remove_flag ,create_time ,creator ,update_time ,mender ,usable_flag ,1 as city_market_id ,2 as table_type from ods.wms__wms_return_trade_201803 ;

drop table if exists temp.wms_return_trade_merge_temp1 ;
create table if not exists temp.wms_return_trade_merge_temp1 as select * ,first_value(table_type) over (partition by return_trade_id order by table_type) as first_table_type from temp.wms_return_trade_merge_temp0 ;

drop table if exists temp.wms_return_trade_merge_temp2 ;
create table if not exists temp.wms_return_trade_merge_temp2 as select * ,date_add('${hivevar:day}',1) as update_date from temp.wms_return_trade_merge_temp1 where table_type=first_table_type ;

insert overwrite table ods.wms_return_trade_merge select return_trade_id ,return_trade_no ,send_trade_no ,ods_return_trade_id ,return_type ,return_express_no ,return_express_id ,return_express_code ,return_express_company ,return_address ,return_phone ,return_reason ,return_reason_type ,return_remark ,return_payment ,return_pay_time ,return_trade_status ,buyer_id ,buyer_nick ,receiver_name ,reveiver_time ,remove_flag ,create_time ,creator ,update_time ,mender ,usable_flag ,city_market_id ,update_date from temp.wms_return_trade_merge_temp2 ;
