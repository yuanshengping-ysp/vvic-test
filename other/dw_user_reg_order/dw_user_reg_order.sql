SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.why_dw_user_reg_order ;
create table temp.why_dw_user_reg_order as select t1.reg_date ,t1.user_id ,t1.reg_time ,t1.reg_terminal ,t2.order_terminal ,t2.min_order_time ,datediff(t2.min_order_time,t1.reg_time) as first_order_days from temp.why_dw_user t1 left join temp.why_dw_user_order t2 on t1.user_id=t2.user_id;
