SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.wms_return_order_merge_temp0 ;
create table if not exists temp.wms_return_order_merge_temp0 as select return_order_id ,ods_return_order_id ,return_trade_id ,return_trade_no ,return_order_status ,supplier_id ,supplier_code ,supplier_name ,supplier_telephone ,supplier_market ,supplier_sub_area ,supplier_floor ,supplier_user_id ,product_id ,product_title ,product_no ,product_price ,purchase_price ,return_price ,product_count ,product_color ,product_size ,sku_id ,sku_code ,pic_path ,return_reason ,return_reason_type ,return_remark ,special_return_note ,special_return_timeout ,remove_flag ,create_time ,creator ,update_time ,mender ,usable_flag ,bid ,refund_image_urls ,1 as table_type from ods.wms__wms_return_order union all select return_order_id ,ods_return_order_id ,return_trade_id ,return_trade_no ,return_order_status ,supplier_id ,supplier_code ,supplier_name ,supplier_telephone ,supplier_market ,supplier_sub_area ,supplier_floor ,supplier_user_id ,product_id ,product_title ,product_no ,product_price ,purchase_price ,return_price ,product_count ,product_color ,product_size ,sku_id ,sku_code ,pic_path ,return_reason ,return_reason_type ,return_remark ,special_return_note ,special_return_timeout ,remove_flag ,create_time ,creator ,update_time ,mender ,usable_flag ,null as bid ,null as refund_image_urls ,2 as table_type from ods.wms__wms_return_order_201803 ;

drop table if exists temp.wms_return_order_merge_temp1 ;
create table if not exists temp.wms_return_order_merge_temp1 as select * ,first_value(table_type) over (partition by return_order_id order by table_type) as first_table_type from temp.wms_return_order_merge_temp0 ;

drop table if exists temp.wms_return_order_merge_temp2 ;
create table if not exists temp.wms_return_order_merge_temp2 as select * ,date_add('${hivevar:day}',1) as update_date from temp.wms_return_order_merge_temp1 where table_type=first_table_type ;

insert overwrite table ods.wms_return_order_merge select return_order_id ,ods_return_order_id ,return_trade_id ,return_trade_no ,return_order_status ,supplier_id ,supplier_code ,supplier_name ,supplier_telephone ,supplier_market ,supplier_sub_area ,supplier_floor ,supplier_user_id ,product_id ,product_title ,product_no ,product_price ,purchase_price ,return_price ,product_count ,product_color ,product_size ,sku_id ,sku_code ,pic_path ,return_reason ,return_reason_type ,return_remark ,special_return_note ,special_return_timeout ,remove_flag ,create_time ,creator ,update_time ,mender ,usable_flag ,bid ,refund_image_urls ,update_date from temp.wms_return_order_merge_temp2 ;
