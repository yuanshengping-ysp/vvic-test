# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi



{
    hive -hivevar day=$today -f   ./dw_fct_pangge/dwd_t_shop_punish_log_daily/dwd_t_shop_punish_log_daily.sql  
} || {
	python ./push_ding.py 'dwd_t_shop_punish_log_daily'
}