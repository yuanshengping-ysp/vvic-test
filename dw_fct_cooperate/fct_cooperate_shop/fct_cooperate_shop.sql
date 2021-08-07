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

