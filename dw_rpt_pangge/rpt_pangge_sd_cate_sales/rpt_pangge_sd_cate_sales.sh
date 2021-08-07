# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi

hive -hivevar day=$today -f  ./dw_rpt_pangge/rpt_pangge_sd_cate_sales/rpt_pangge_sd_cate_sales.sql