SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.fct_cooperate_shop_temp0 ;
create table if not exists temp.fct_cooperate_shop_temp0 as select '${hivevar:day}' as stati_date ;


drop table if exists temp.fct_cooperate_shop_temp1 ;
create table if not exists temp.fct_cooperate_shop_temp1 as select * ,row_number() over(partition by shop_id order by update_date) as rank from ods.vvic__cooperate_shop_his where id is not null ;

drop table if exists temp.fct_cooperate_shop_temp2 ;
create table if not exists temp.fct_cooperate_shop_temp2 as select t1.* ,t2.stati_date ,(case when t1.is_valid=1 and t2.stati_date<=t1.update_date and t2.stati_date>=t1.create_date and t1.status=1 and t2.stati_date>=t1.effective_ymd then 1 when t1.is_valid=1 and t2.stati_date<=t1.update_date and t2.stati_date>=t1.create_date and t1.status=0 and t2.stati_date>=t1.effective_ymd and t2.stati_date<substring(t1.invalid_time,1,10) then 1 when t1.is_valid=0 and t2.stati_date<t1.update_date and t2.stati_date>=t1.create_date and t1.status=1 and t2.stati_date>=t1.effective_ymd then 1 when t1.is_valid=0 and t2.stati_date<t1.update_date and t2.stati_date>=t1.create_date and t1.status=0 and t2.stati_date>=t1.effective_ymd and t2.stati_date<substring(t1.invalid_time,1,10) then 1 when t1.rank=1 and t2.stati_date<'2019-11-21' and t1.status=1 and t2.stati_date>=t1.effective_ymd then 1 when t1.rank=1 and t2.stati_date<'2019-11-21' and t1.status=0 and t2.stati_date>=t1.effective_ymd and t2.stati_date<substring(t1.invalid_time,1,10) then 1 else 0 end) as is_coo_shop from temp.fct_cooperate_shop_temp1 t1 ,temp.fct_cooperate_shop_temp0 t2 ;

drop table if exists temp.fct_cooperate_shop_temp3 ;
create table if not exists temp.fct_cooperate_shop_temp3 as select distinct shop_id ,lower_price ,effective_ymd as effective_date ,invalid_time as invalid_date ,stati_date as `date` ,is_coo_shop as coo_shop_num from temp.fct_cooperate_shop_temp2 where is_coo_shop=1 ;

insert overwrite table dw.fct_cooperate_shop partition(`date`='${hivevar:day}') select shop_id ,lower_price ,effective_date ,invalid_date ,coo_shop_num from temp.fct_cooperate_shop_temp3 ;

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




drop table if exists temp.rpt_pangge_shop_upshelf_daily_temp0 ;
create table if not exists temp.rpt_pangge_shop_upshelf_daily_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.rpt_pangge_shop_upshelf_daily_temp1 ;
create table if not exists temp.rpt_pangge_shop_upshelf_daily_temp1 as select distinct t1.`date` ,t1.shop_id from (select distinct substring(up_time,1,10) as `date` ,shop_id from ods.t_item where up_time>=date_sub('${hivevar:day}',6) and up_time<date_add('${hivevar:day}',1) union all select distinct substring(up_shelf_time,1,10) as `date` ,shop_id from ods.t_item where up_shelf_time>=date_sub('${hivevar:day}',6) and up_shelf_time<date_add('${hivevar:day}',1)) t1;

drop table if exists temp.rpt_pangge_shop_upshelf_daily_temp2 ;
create table if not exists temp.rpt_pangge_shop_upshelf_daily_temp2 as select t1.* ,t2.stati_date from temp.rpt_pangge_shop_upshelf_daily_temp1 t1 ,temp.rpt_pangge_shop_upshelf_daily_temp0 t2 where datediff(t2.stati_date,t1.`date`) between 0 and 6 ;

drop table if exists temp.rpt_pangge_shop_upshelf_daily_temp3 ;
create table if not exists temp.rpt_pangge_shop_upshelf_daily_temp3 as select t1.stati_date as `date` ,t1.shop_id ,(case when t2.days>1 then 1 else 0 end) as upshelf_twice_1w from (select stati_date ,shop_id from temp.rpt_pangge_shop_upshelf_daily_temp2 where stati_date=`date`) t1 left join (select stati_date ,shop_id ,count(1) as days from temp.rpt_pangge_shop_upshelf_daily_temp2 group by stati_date ,shop_id ) t2 on t1.stati_date=t2.stati_date and t1.shop_id=t2.shop_id ;

drop table if exists temp.rpt_pangge_shop_upshelf_daily_temp4 ;
create table if not exists temp.rpt_pangge_shop_upshelf_daily_temp4 as select t6.name as city_market_name ,t6.id as city_market_id ,t4.bid ,t4.name as market_name ,t1.shop_id ,t2.name as shop_name ,t2.`position` ,t2.telephone ,t2.wechat ,t2.qq ,(case when t3.shop_id is null then 0 else 1 end) as is_coo_shop ,t1.`date` ,t1.upshelf_twice_1w ,t5.ranking_month as shop_rank from temp.rpt_pangge_shop_upshelf_daily_temp3 t1 left join  ods.t_shop t2 on t1.shop_id=t2.id left join  (select * from rpt.rpt_cooperate_shop where `date`>='${hivevar:day}' and `date`<='${hivevar:day}' and coo_shop_num=1) t3 on t1.`date`=t3.`date` and cast(t1.shop_id as string) =t3.shop_id left join  (select distinct bid,name  from ods.t_market) t4 on t2.bid=t4.bid left join (select * from rpt.rpt_stat_shop_rank_popular where `date`>='${hivevar:day}' and `date`<='${hivevar:day}' and popular_type=0) t5 on t1.shop_id=t5.shop_id and t1.`date`=t5.`date` left join  ods.t_city_market t6 on t2.city_market_id=t6.id ;

insert overwrite table rpt.rpt_pangge_shop_upshelf_daily partition(`date`='${hivevar:day}') select `city_market_id` ,`city_market_name` ,`bid` ,`market_name` ,`shop_id` ,`shop_name` ,`position` ,`telephone` ,`wechat` ,`qq` ,`is_coo_shop` ,`upshelf_twice_1w` ,`shop_rank` ,0 as `is_new_shop` from temp.rpt_pangge_shop_upshelf_daily_temp4 ;

drop table if exists temp.rpt_pangge_shop_upshelf_daily_temp5 ;
create table if not exists temp.rpt_pangge_shop_upshelf_daily_temp5 as select t1.* ,(case when t1.`date`=t2.min_date then 1 else 0 end) as is_new_shop from temp.rpt_pangge_shop_upshelf_daily_temp4 t1 left join (select shop_id ,min(`date`) as min_date from rpt.rpt_pangge_shop_upshelf_daily group by shop_id) t2 on t1.shop_id=t2.shop_id ;

insert overwrite table rpt.rpt_pangge_shop_upshelf_daily partition(`date`='${hivevar:day}') select `city_market_id` ,`city_market_name` ,`bid` ,`market_name` ,`shop_id` ,`shop_name` ,`position` ,`telephone` ,`wechat` ,`qq` ,`is_coo_shop` ,`upshelf_twice_1w` ,`shop_rank` ,`is_new_shop` from temp.rpt_pangge_shop_upshelf_daily_temp5 where `date`='${hivevar:day}' ;

