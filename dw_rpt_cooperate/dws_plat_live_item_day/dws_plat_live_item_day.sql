
set hive.tez.container.size=1024;
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled=false ;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
SET mapred.max.split.size=268435456; 
SET mapred.min.split.size.per.node=134217728 ; 
SET mapred.min.split.size.per.rack=134217728 ; 
SET hive.merge.mapfiles = true ;
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles = true ; 
SET hive.merge.size.per.task = 268435456 ;
SET hive.merge.smallfiles.avgsize=268435456 ;



--累计下单件数>=30件的档口(搜款网代发)
--48小时有效入仓率(累计口径)>=88%的档口(搜款网代发)
--累计实发件数>=100件数的档口【18年12月起算】(搜款网代发)
--累计工单次品率<=1%的档口【18年12月起算】(搜款网代发)
--剔除【高级质检累计件数>=20件，同时次品率>10%】的档口




drop table  temp.dws_plat_live_item_day_temp01;

--累计下单件数>=30件的档口(搜款网代发)
--48小时有效入仓率(累计口径)>=88%的档口(搜款网代发)

create table temp.dws_plat_live_item_day_temp01 as 
select t1.shop_id,t2.receive_item_num/t1.pay_item_num valid_receive_item_ratio_7d from 
(select shop_id,sum(pay_item_num) pay_item_num 
from dws.dws_pangge_shop_pay_day
group by shop_id )t1
left join
(select shop_id,sum(receive_item_num) receive_item_num 
from dws.dws_pangge_shop_order_day
group by shop_id) t2
on t1.shop_id=t2.shop_id
where t1.pay_item_num>=30 
and t2.receive_item_num/t1.pay_item_num>=0.88;






drop table  temp.dws_plat_live_item_day_temp02;
--累计工单次品率<=1%的档口【18年12月起算】(搜款网代发)
--累计实发件数>=100件数的档口【18年12月起算】(搜款网代发)
---- 质量问题
 ---- 实发件数
create table temp.dws_plat_live_item_day_temp02 as 
select t1.shop_id from 
(select s1.shop_id
--实发件数
-- ,count(distinct s1.product_label_no) receive_item_num 
,sum(s1.`ship_item_num`) as `receive_item_num`
from dw.fct_pangge_ship_item s1
-- from dwd.dwd_df_ship_label sl    --搜款网代发
-- where s1.order_type = 0
--  and substr(`date`,1,4) >= '2019'
where s1.`date`>='2019-12-17'
group by s1.shop_id) t1
left join
(select t.shop_id
--质量问题工单件数
,sum(product_count) abnormal_prodeuct_num
from (select sku_id,product_count from ods.wms__wms_return_order where return_reason = '质量问题'
      union all
      select sku_id,product_count from ods.wms__wms_return_order_his where return_reason = '质量问题' and substr(create_time,1,4) >= '2019'
     )om    --退款工单
left join ods.t_item_sku i
  on om.sku_id  = i.id
left join ods.t_item t
  on i.item_id = t.id
group by t.shop_id) t2
on t1.shop_id=t2.shop_id
where nvl(t2.abnormal_prodeuct_num,0)/t1.receive_item_num <0.01
and t1.receive_item_num >=100;




drop table  temp.dws_plat_live_item_day_temp03;
--剔除【高级质检累计件数>=20件，同时次品率>10%】的档口
---- 高级质检
create table temp.dws_plat_live_item_day_temp03 as 
select distinct t1.shop_id from (
select od.shop_id
--累计高级质检总件数
,count(distinct pl.product_label_no) total_vip_quality_num
--累计高级质检次品件数
,count(
case when pl.QC_STATUS = 9 
then pl.product_label_no end) as total_vip_no_quality_num
from ods.wms_product_label_merge pl
left join ods.vvic_order_details_merge od
  on pl.ods_order_id = od.order_details_id
left join ods.vvic_order_merge o
  on od.order_id = o.order_id
left join ods.wms_trade_subject ts
  on pl.trade_id = ts.trade_id
inner join ods.wms__wms_trade_vas tv   --质检类型
  on ts.trade_no = tv.trade_no
 and tv.type = 2 
 and tv.`service_name` <> '基础质检'
 and o.order_type = 0
group by od.shop_id )t1
where total_vip_no_quality_num/total_vip_quality_num >0.1
and total_vip_quality_num >=20;


drop table  temp.dws_plat_live_item_day_temp04;
---- 近三天销量
create table temp.dws_plat_live_item_day_temp04 as 
select 
shop_id
,item_id
--近3天销量
,sum(distinct pay_item_num) pay_item_num_3d 
--近3天下单用户数
,count(distinct user_id) pay_user_num_3d
-- from dwd.dwd_pangge_fct_pay_label  
-- where order_type = '0'
--  and `date` between date_sub('${hivevar:day}',3) and '${hivevar:day}'   --近三天
from dw.fct_pangge_pay_order_item
where `date`> date_sub('${hivevar:day}',3) and `date`<='${hivevar:day}'
group by shop_id,item_id;

drop table  temp.dws_plat_live_item_day_temp05;



--有效档口
create table temp.dws_plat_live_item_day_temp05 as 
select t1.shop_id ,t1.valid_receive_item_ratio_7d
from temp.dws_plat_live_item_day_temp01 t1
join
temp.dws_plat_live_item_day_temp02 t2
on t1.shop_id=t2.shop_id
left join
temp.dws_plat_live_item_day_temp03 t3
on t1.shop_id=t3.shop_id
where t3.shop_id is null ;





--取有效档口对应得有效商品
INSERT overwrite table dws.dws_plat_live_item_day  partition(`date`='${hivevar:day}')
select t2.item_id,t2.vcid,t1.shop_id,
nvl(t2.pay_item_num_3d,0) pay_item_num_3d,nvl(t2.pay_user_num_3d,0) pay_user_num_3d,t2.up_time,t1.valid_receive_item_ratio_7d from 
temp.dws_plat_live_item_day_temp05 t1
left join
(select a.*,c.pay_item_num_3d,c.pay_user_num_3d from 
(select id item_id,shop_id,vcid,up_time,price,index_img_url 
from ods.t_item 
where status=1 and pangge_flag=1 
and spam=0 and is_pre_sell=0 ) a
left join
(select vcid,percentile_approx(price,0.99) quantile_price
from ods.t_item where status=1 group by vcid) b
on a.vcid=b.vcid
left join
temp.dws_plat_live_item_day_temp04 c
on a.item_id=c.item_id
where a.price<b.quantile_price
)t2
on t1.shop_id=t2.shop_id
left join
(select item_id,sum(is_bad_img) is_bad_img from 
(select  item_id,
--case when image_mosaic_score>0 then 1
case when image_collage_score>0 then 1
when image_text_score>0 then 1
when image_detail_score>0 then 1
when image_border_score>0 then 1
else 0 end is_bad_img,img_url,max(create_time)
from ods.t_sp_item_img
group by item_id,
--case when image_mosaic_score>0 then 1
case when image_collage_score>0 then 1
when image_text_score>0 then 1
when image_detail_score>0 then 1
when image_border_score>0 then 1
else 0 end ,img_url)t
group by item_id) t3
on t2.item_id=t3.item_id 
where t2.item_id is not null 
and t3.is_bad_img =0;






