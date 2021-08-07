SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.why_rpt_reg_order ;
create table temp.why_rpt_reg_order as select reg_date,'所有' as reg_terminal ,count(distinct user_id) as reg_user_num ,count(distinct (case when first_order_days is not null then user_id else null end)) as order_user_num ,count(distinct (case when first_order_days=0 then user_id else null end)) as order_user_num_0 ,count(distinct (case when first_order_days<7 then user_id else null end)) as order_user_num_7 ,count(distinct (case when first_order_days<30 then user_id else null end)) as order_user_num_30 from temp.why_dw_user_reg_order group by reg_date ;

insert into table temp.why_rpt_reg_order select reg_date,reg_terminal ,count(distinct user_id) as reg_user_num ,count(distinct (case when first_order_days is not null then user_id else null end)) as order_user_num ,count(distinct (case when first_order_days=0 then user_id else null end)) as order_user_num_0 ,count(distinct (case when first_order_days<7 then user_id else null end)) as order_user_num_7 ,count(distinct (case when first_order_days<30 then user_id else null end)) as order_user_num_30 from temp.why_dw_user_reg_order group by reg_date,reg_terminal ;
