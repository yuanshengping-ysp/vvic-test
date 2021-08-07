SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists temp.fct_pangge_pay_order_temp0 ;
create table if not exists temp.fct_pangge_pay_order_temp0 as select '${hivevar:day}' as stati_date ;

drop table if exists temp.fct_pangge_pay_order_temp1 ;
create table if not exists temp.fct_pangge_pay_order_temp1 as select `date` ,pay_time ,city_market_id ,user_id ,order_type ,order_no ,pay_item_money ,pay_order_money ,sum(pay_item_num) as pay_item_num ,sum(item_sku_price*pay_item_num) as item_sku_money from dw.fct_pangge_pay_order_item where `date`>='${hivevar:day}' and `date`<=date_add('${hivevar:day}',1) group by `date` ,pay_time ,city_market_id ,user_id ,order_type ,order_no ,pay_item_money ,pay_order_money ;

drop table if exists temp.fct_pangge_pay_order_temp2 ;
create table if not exists temp.fct_pangge_pay_order_temp2 as select t1.* from temp.fct_pangge_pay_order_temp1 t1 inner join temp.fct_pangge_pay_order_temp0 t2 on t1.`date`=t2.stati_date ;

insert overwrite table dw.fct_pangge_pay_order partition(`date`='${hivevar:day}') select pay_time ,city_market_id ,user_id ,order_type ,order_no ,pay_item_money ,pay_order_money ,pay_item_num ,item_sku_money from temp.fct_pangge_pay_order_temp2 ;
