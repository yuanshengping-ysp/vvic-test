SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.wms_return_label_merge_temp0 ;
create table if not exists temp.wms_return_label_merge_temp0 as select return_label_id ,return_trade_id ,return_order_id ,return_batch_id ,storage_location_id ,return_batch_no ,return_label_no ,return_label_status ,inventory_status ,buy_price ,return_price ,receive_time ,receive_name ,reason ,return_time ,return_name ,label_return_reason_type ,label_return_reason_remark ,label_return_reason ,judgment_type ,judgment ,resp_party_type ,resp_party ,return_label_remark ,return_fail_reason ,change_price_reason ,put_in_storage_time ,check_out_storage_time ,put_in_storage_person ,check_out_storage_person ,multi_return_status ,multi_return_remark ,lost_register_time ,lost_registrant ,lost_remark ,checker ,check_time ,check_status ,remove_flag ,create_time ,creator ,update_time ,mender ,usable_flag ,handle_type ,put_in_city_market_id ,1 as table_type from ods.wms__wms_return_label union all select return_label_id ,return_trade_id ,return_order_id ,return_batch_id ,storage_location_id ,return_batch_no ,return_label_no ,return_label_status ,inventory_status ,buy_price ,return_price ,receive_time ,receive_name ,reason ,return_time ,return_name ,label_return_reason_type ,label_return_reason_remark ,label_return_reason ,judgment_type ,judgment ,resp_party_type ,resp_party ,return_label_remark ,return_fail_reason ,change_price_reason ,put_in_storage_time ,check_out_storage_time ,put_in_storage_person ,check_out_storage_person ,multi_return_status ,multi_return_remark ,lost_register_time ,lost_registrant ,lost_remark ,checker ,check_time ,check_status ,remove_flag ,create_time ,creator ,update_time ,mender ,usable_flag ,null as handle_type ,null as put_in_city_market_id ,2 as table_type from ods.wms__wms_return_label_201803 ;

drop table if exists temp.wms_return_label_merge_temp1 ;
create table if not exists temp.wms_return_label_merge_temp1 as select * ,first_value(table_type) over (partition by return_label_id order by table_type) as first_table_type from temp.wms_return_label_merge_temp0 ;

drop table if exists temp.wms_return_label_merge_temp2 ;
create table if not exists temp.wms_return_label_merge_temp2 as select * ,date_add('${hivevar:day}',1) as update_date from temp.wms_return_label_merge_temp1 where table_type=first_table_type ;

insert overwrite table ods.wms_return_label_merge select return_label_id ,return_trade_id ,return_order_id ,return_batch_id ,storage_location_id ,return_batch_no ,return_label_no ,return_label_status ,inventory_status ,buy_price ,return_price ,receive_time ,receive_name ,reason ,return_time ,return_name ,label_return_reason_type ,label_return_reason_remark ,label_return_reason ,judgment_type ,judgment ,resp_party_type ,resp_party ,return_label_remark ,return_fail_reason ,change_price_reason ,put_in_storage_time ,check_out_storage_time ,put_in_storage_person ,check_out_storage_person ,multi_return_status ,multi_return_remark ,lost_register_time ,lost_registrant ,lost_remark ,checker ,check_time ,check_status ,remove_flag ,create_time ,creator ,update_time ,mender ,usable_flag ,handle_type ,put_in_city_market_id ,update_date from temp.wms_return_label_merge_temp2 ;
