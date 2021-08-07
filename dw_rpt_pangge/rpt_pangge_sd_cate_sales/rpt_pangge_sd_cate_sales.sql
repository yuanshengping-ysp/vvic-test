SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists rpt.rpt_pangge_sd_cate_sales;
--类目销量表，rpt_pangge_sd_cate_sales
create table rpt.rpt_pangge_sd_cate_sales as 
 select t1.ad_cate_id,t1.vcid,t2.vcname,t1.sales_num_3d,t1.sales_item_num_3d,t1.`date` from 
 (select ad_cate_id,vcid,sum(sales_num_3d) sales_num_3d,count(distinct item_id) sales_item_num_3d,`date` 
 from rpt.rpt_pangge_sd_item_sales 
group by ad_cate_id,vcid ,`date`
order by sales_num_3d desc )t1
left join
ods.t_category_vvic t2
on t1.vcid=t2.vcid
where t1.sales_item_num_3d >10