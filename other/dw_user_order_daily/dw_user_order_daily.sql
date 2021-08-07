SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.why_dw_user_order_daily;
create table temp.why_dw_user_order_daily as select t.order_date ,t.user_id ,sum(t.order_num) as order_num ,sum(t.item_num) as item_num ,sum(t.order_money) as order_money from (select substring(create_time,1,10) as order_date ,user_id ,count(distinct order_id) as order_num ,sum(initial_count) as item_num ,sum(paid_amount) as order_money from ods.vvic_order group by substring(create_time,1,10) ,user_id union all select substring(create_time,1,10) as order_date ,user_id ,count(distinct order_id) as order_num ,sum(initial_count) as item_num ,sum(paid_amount) as order_money  from ods.vvic_order_201803 group by substring(create_time,1,10) ,user_id union all select substring(create_time,1,10) as order_date ,user_id ,count(distinct order_id) as order_num ,sum(initial_count) as item_num ,sum(paid_amount) as order_money from ods.vvic_order_archive group by substring(create_time,1,10) ,user_id) t group by t.order_date ,t.user_id ;
