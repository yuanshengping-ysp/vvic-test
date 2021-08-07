SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

drop table if exists rpt.rpt_pangge_sd_user_cate_rec;

--用户类目推荐表，rpt_pangge_sd_user_cate_rec
create table rpt.rpt_pangge_sd_user_cate_rec as 
 select case when length(t3.uid) >10 then '游客' else '用户' end user_type,
 t3.uid,concat_ws(',',t3.vcid_list) vcid_list,'${hivevar:day}' `date` from 
( select t1.uid,collect_set(cast(t2.vcid as string)) vcid_list from 
(select uid,ad_cate_id from  label.dw_label 
where `date`='${hivevar:day}' ) t1
left join
rpt.rpt_pangge_sd_cate_sales t2
on t1.ad_cate_id=t2.ad_cate_id
group by t1.uid) t3