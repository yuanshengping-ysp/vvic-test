SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists rpt.rpt_pangge_sd_shop_sales;


------档口备货率表，rpt_pangge_sd_shop_sales------
create table rpt.rpt_pangge_sd_shop_sales as
select t1.*,t3.sales_num_3d/3 sales_num_avg_3d,t3.sales_num_3d,t4.return_success_ratio_2w from 
(select shop_id,pay_item_num_2w/14 sales_num_avg_2w,allot_ratio_2w,opp_allot_ratio_2w
from dws.dws_pangge_shop_allot_day 
  where `date`='${hivevar:day}')t1
 join
 (select shop_id from dw.fct_cooperate_shop 
 where `date`='${hivevar:day}' and coo_shop_num=1)t2
 on t1.shop_id  =t2.shop_id
 left join
 (select shop_id,sum(sales_num_3d) sales_num_3d from rpt.rpt_pangge_sd_item_sales group by shop_id) t3
  on t1.shop_id  =t3.shop_id
 left join
(select `date`,shop_id,sum(return_success_item_num)/sum(return_item_num) return_success_ratio_2w
from rpt.rpt_pangge_pay_daily
where `date`=date_sub('${hivevar:day}',7)
and shop_id<>0 and period=168
group by `date`,shop_id) t4
on t1.shop_id=t4.shop_id
