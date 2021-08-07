# 参数：
# args[0] ,数据日期,日期格式yyyy-MM-dd

if [ $# == 1 ]; then
   today=$1
else 
   today=`date -d -1days '+%Y-%m-%d'`
fi


{
    hive -hivevar day=$today -f   ./dw_dim/dim_wms_product_label/dim_wms_product_label.sql 
} || {
    echo 'task fail:dim_wms_product_label !!!!!!'
	python ./push_ding.py 'dim_wms_product_label'
}