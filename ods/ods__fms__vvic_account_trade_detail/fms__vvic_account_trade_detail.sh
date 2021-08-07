
{
    hive -e "truncate table ods.fms__vvic_account_trade_detail"
    python /root/soft/datax/datax/bin/datax.py ./ods/ods__fms__vvic_account_trade_detail/fms__vvic_account_trade_detail.json
} || {
    echo 'task fail:fms__vvic_account_trade_detail.json !!!!!!'
	python ./common/push_ding.py 'fms__vvic_account_trade_detail.json'
}
