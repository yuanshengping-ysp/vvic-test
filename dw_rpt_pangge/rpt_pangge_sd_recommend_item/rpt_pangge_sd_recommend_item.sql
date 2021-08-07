SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists rpt.rpt_pangge_sd_recommend_item;


----用户商品推荐表，rpt_pangge_sd_recommend_item----
create table rpt.rpt_pangge_sd_recommend_item as 
select t1.*,t4.allot_ratio_1w,t4.pay_item_num_1w,t5.title,t5.index_img_url from 
(select user_id,item_id from 
temp.rpt_recommend_item  lateral view explode(split(item_list,',')) item_list as item_id
where `date`='${hivevar:day}' and user_id !='-' and user_id is not null ) t1
 join
rpt.rpt_pangge_sd_item_sales t2
on t1.item_id=t2.item_id
 join
 (select shop_id from dw.fct_cooperate_shop 
 where `date`='${hivevar:day}' and coo_shop_num=1)t3
on t2.shop_id=t3.shop_id
left join
(select * from dws.dws_pangge_item_allot_day
 where `date`='${hivevar:day}' ) t4
on t1.item_id=t4.item_id
left join
ods.t_item t5
on t1.item_id=t5.id
where t4.allot_ratio_1w>0.5 and t4.pay_item_num_1w>5 and t5.up_time >date_sub('${hivevar:day}',8)