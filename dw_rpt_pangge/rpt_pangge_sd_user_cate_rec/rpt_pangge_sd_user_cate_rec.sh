# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi

hive -hivevar day=$today -f  ./dw_rpt_pangge/rpt_pangge_sd_user_cate_rec/rpt_pangge_sd_user_cate_rec.sql