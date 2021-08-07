SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;


drop table if exists temp.why_rpt_user ;
create table temp.why_rpt_user as select reg_date,'所有' as reg_terminal,'所有' as is_shop_user ,count(distinct user_id) as user_num from temp.why_dw_user group by reg_date ;

insert into table temp.why_rpt_user select reg_date,'所有' as reg_terminal,is_shop_user ,count(distinct user_id) as user_num from temp.why_dw_user group by reg_date,is_shop_user ;

insert into table temp.why_rpt_user select reg_date,reg_terminal,'所有' as is_shop_user ,count(distinct user_id) as user_num from temp.why_dw_user group by reg_date,reg_terminal ;

insert into table temp.why_rpt_user select reg_date,reg_terminal,is_shop_user ,count(distinct user_id) as user_num from temp.why_dw_user group by reg_date,reg_terminal,is_shop_user ;
