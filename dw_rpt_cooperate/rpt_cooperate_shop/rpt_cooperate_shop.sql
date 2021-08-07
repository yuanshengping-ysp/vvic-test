SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.rpt_cooperate_shop_temp0 ;
create table if not exists temp.rpt_cooperate_shop_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.rpt_cooperate_shop_temp1 ;
create table if not exists temp.rpt_cooperate_shop_temp1 as select t2.stati_date ,t1.shop_id ,sum(case when t2.stati_date>=t1.`date` then 1 else 0 end) as coo_days from dw.fct_cooperate_shop t1 ,temp.rpt_cooperate_shop_temp0 t2 group by t2.stati_date ,t1.shop_id having coo_days>0 ;

drop table if exists temp.rpt_cooperate_shop_temp2 ;
create table if not exists temp.rpt_cooperate_shop_temp2 as select `date` ,shop_id ,lower_price ,1 as coo_shop_num from dw.fct_cooperate_shop where `date`>='${hivevar:day}' and `date`<='${hivevar:day}' ;

drop table if exists temp.rpt_cooperate_shop_temp3 ;
create table if not exists temp.rpt_cooperate_shop_temp3 as select date_add(`date`,1) as stati_date ,`date` ,shop_id from dw.fct_cooperate_shop where `date`>=date_sub('${hivevar:day}',1) and `date`<=date_sub('${hivevar:day}',1) ;

drop table if exists temp.rpt_cooperate_shop_temp4 ;
create table if not exists temp.rpt_cooperate_shop_temp4 as select t1.stati_date ,t1.shop_id ,t1.coo_days ,t2.lower_price ,(case when t2.coo_shop_num is null then 0 else t2.coo_shop_num end) as coo_shop_num ,(case when t3.shop_id is null then 0 else 1 end) as coo_shop_num_yesterday from temp.rpt_cooperate_shop_temp1 t1 left join temp.rpt_cooperate_shop_temp2 t2 on t1.stati_date=t2.`date` and t1.shop_id=t2.shop_id left join temp.rpt_cooperate_shop_temp3 t3 on t1.stati_date=t3.stati_date and t1.shop_id=t3.shop_id ;

drop table if exists temp.rpt_cooperate_shop_temp5 ;
create table if not exists temp.rpt_cooperate_shop_temp5 as select t1.stati_date as `date` ,t1.shop_id ,t2.name as shop_name ,t3.bid ,t3.name as market_name ,t1.coo_days ,t1.lower_price ,t1.coo_shop_num ,(case when t1.coo_shop_num=1 and t1.coo_days=1 then 1 else 0 end) as new_coo_shop_num ,(case when t1.coo_shop_num=0 and t1.coo_shop_num_yesterday=1 then 1 else 0 end) as exit_coo_shop_num ,(case when t1.coo_shop_num=1 and t1.coo_shop_num_yesterday=0 and t1.coo_days>1 then 1 else 0 end) as twice_coo_shop_num from temp.rpt_cooperate_shop_temp4 t1 left join ods.t_shop t2 on t1.shop_id=t2.id left join (select distinct bid,name from ods.t_market) t3 on t2.bid=t3.bid ;

insert overwrite table rpt.rpt_cooperate_shop partition(`date`='${hivevar:day}') select shop_id ,shop_name ,bid ,market_name ,lower_price ,coo_shop_num ,new_coo_shop_num ,twice_coo_shop_num ,exit_coo_shop_num ,coo_days from temp.rpt_cooperate_shop_temp5 ;

