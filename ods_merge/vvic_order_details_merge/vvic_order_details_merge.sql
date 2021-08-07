SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.vvic_order_details_merge_temp0 ;
create table if not exists temp.vvic_order_details_merge_temp0 as select order_details_id ,order_id ,order_no ,item_id ,item_name ,item_img ,item_sn ,item_num ,return_num ,sku_id ,sku_price ,sku_color ,sku_size ,shop_id ,shop_name ,shop_ww_nickname ,shop_position ,shop_qq ,shop_wechat ,shop_telephone ,shop_market ,shop_floor ,shop_subarea ,purchase_price ,user_id ,has_fittings ,has_purchase_num ,not_purchase_num ,ing_purchase_num ,wait_purchase_num ,lack_reason ,arrive_time ,delivery_status ,parent_id ,total_amount ,status ,return_note ,return_time_out ,create_time ,update_time ,vesion ,bid ,city_market_id ,city_market_code ,item_vid ,sku_vid ,subject_id ,subject_name ,cost_price ,initial_count ,1 as table_type from ods.vvic_order_details_subject union all select order_details_id ,order_id ,order_no ,item_id ,item_name ,item_img ,item_sn ,item_num ,return_num ,sku_id ,sku_price ,sku_color ,sku_size ,shop_id ,shop_name ,shop_ww_nickname ,shop_position ,shop_qq ,shop_wechat ,shop_telephone ,shop_market ,shop_floor ,shop_subarea ,purchase_price ,user_id ,has_fittings ,has_purchase_num ,not_purchase_num ,ing_purchase_num ,wait_purchase_num ,lack_reason ,arrive_time ,delivery_status ,parent_id ,total_amount ,status ,return_note ,return_time_out ,create_time ,update_time ,vesion ,0 as bid ,1 as city_market_id ,'gz' as city_market_code ,null as item_vid ,null as sku_vid ,null as subject_id ,null as subject_name ,null as cost_price ,null as initial_count ,2 as table_type from ods.ods__vvic_order_details_201803 union all select order_details_id ,order_id ,order_no ,item_id ,item_name ,item_img ,item_sn ,item_num ,return_num ,sku_id ,sku_price ,sku_color ,sku_size ,shop_id ,shop_name ,shop_ww_nickname ,shop_position ,shop_qq ,shop_wechat ,shop_telephone ,shop_market ,shop_floor ,shop_subarea ,purchase_price ,user_id ,has_fittings ,has_purchase_num ,not_purchase_num ,ing_purchase_num ,wait_purchase_num ,lack_reason ,arrive_time ,delivery_status ,parent_id ,total_amount ,status ,return_note ,return_time_out ,create_time ,update_time ,null as vesion ,null as bid ,1 as city_market_id ,'gz' as city_market_code ,null as item_vid ,null as sku_vid ,null as subject_id ,null as subject_name ,null as cost_price ,null as initial_count ,3 as table_type from ods.ods__vvic_order_details_archive ;

drop table if exists temp.vvic_order_details_merge_temp1 ;
create table if not exists temp.vvic_order_details_merge_temp1 as select * ,first_value(table_type) over (partition by order_details_id order by table_type) as first_table_type from temp.vvic_order_details_merge_temp0 ;

drop table if exists temp.vvic_order_details_merge_temp2 ;
create table if not exists temp.vvic_order_details_merge_temp2 as select * ,date_add('${hivevar:day}',1) as update_date from temp.vvic_order_details_merge_temp1 where table_type=first_table_type ;

insert overwrite table ods.vvic_order_details_merge select order_details_id ,order_id ,order_no ,item_id ,item_name ,item_img ,item_sn ,item_num ,return_num ,sku_id ,sku_price ,sku_color ,sku_size ,shop_id ,shop_name ,shop_ww_nickname ,shop_position ,shop_qq ,shop_wechat ,shop_telephone ,shop_market ,shop_floor ,shop_subarea ,purchase_price ,user_id ,has_fittings ,has_purchase_num ,not_purchase_num ,ing_purchase_num ,wait_purchase_num ,lack_reason ,arrive_time ,delivery_status ,parent_id ,total_amount ,status ,return_note ,return_time_out ,create_time ,update_time ,vesion ,bid ,city_market_id ,city_market_code ,item_vid ,sku_vid ,subject_id ,subject_name ,cost_price ,initial_count ,update_date from temp.vvic_order_details_merge_temp2 ;


