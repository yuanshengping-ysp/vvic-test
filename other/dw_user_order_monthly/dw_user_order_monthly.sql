SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.why_dw_user_order_monthly;
create table temp.why_dw_user_order_monthly as select substring(regexp_replace(order_date,'-',''),1,6) as order_month ,user_id ,sum(order_num) as order_num ,sum(item_num) as item_num ,sum(order_money) as order_money from temp.why_dw_user_order_daily group by substring(regexp_replace(order_date,'-',''),1,6) ,user_id;
