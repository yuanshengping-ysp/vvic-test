# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi



{
    hive -hivevar day=$today -f   ./dw_fct_pangge/dws_pangge_mo_rate_valid_shop/dws_pangge_mo_rate_valid_shop.sql  
} || {
	python ./push_ding.py 'dws_pangge_mo_rate_valid_shop'
}