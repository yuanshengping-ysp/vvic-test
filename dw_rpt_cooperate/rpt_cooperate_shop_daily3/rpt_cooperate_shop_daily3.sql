-- 常规参数设置
SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;
SET hive.exec.dynamic.partition=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict ;
SET hive.strict.checks.type.safety=FALSE;
SET hive.execution.engine=tez;
SET hive.tez.container.size=1024;
SET hive.vectorized.execution.enabled=FALSE;

-- 

INSERT OVERWRITE TABLE dwd.dwd_cooperate_shop_basic_details PARTITION(`date`='${hivevar:day}')
SELECT
t1.shop_id,t2.name AS shop_name,position AS shop_position,t2.bid,t3.name AS market_name,print_type,print_type_name
FROM
(SELECT shop_id FROM dw.fct_cooperate_shop WHERE `date`='${hivevar:day}' AND coo_shop_num=1) t1

LEFT JOIN
(SELECT id,bid,`position`,name FROM ods.t_shop) t2
ON t1.shop_id=t2.id

LEFT JOIN
(SELECT DISTINCT bid,name FROM ods.t_market) t3
ON t2.bid=t3.bid

LEFT JOIN
(SELECT shop_id,print_type,
CASE WHEN print_type=1 THEN '派标' WHEN print_type=2 THEN '打标' END AS print_type_name,
CASE WHEN stop_print_time='' THEN '17:00:00' ELSE stop_print_time END AS stop_print_time 
FROM ods.vvic__cooperate_shop) t4
ON t1.shop_id=t4.shop_id ;



INSERT OVERWRITE TABLE rpt.rpt_cooperate_shop_daily3 PARTITION(`date`='${hivevar:day}')
SELECT
*
FROM
(SELECT 
t1.shop_id,shop_name,shop_position,CAST (bid AS bigint) AS bid,market_name,CAST(print_type AS int) AS print_type,print_type_name,
NVL(enallot_item_num,0) AS enallot_item_num,
NVL(allot_item_num,0) AS allot_item_num,
NVL(receive_item_num,0) AS receive_item_num,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS insert_time
FROM
(SELECT * FROM dwd.dwd_cooperate_shop_basic_details WHERE `date`='${hivevar:day}') t1

LEFT JOIN
(SELECT
shop_id,
COUNT(CASE WHEN current_life_status=1 THEN 1 ELSE NULL END) AS allot_item_num
FROM
(SELECT
t1.*,t2.shop_id
FROM
(SELECT * FROM ods.wms__wms_product_label_log WHERE to_date(create_time)='${hivevar:day}') t1

JOIN
(SELECT product_label_no,shop_id FROM dw.dim_wms_product_label) t2
ON t1.product_label_no=t2.product_label_no) temp
GROUP BY shop_id) t2
ON t1.shop_id=t2.shop_id

LEFT JOIN
(SELECT
shop_id,COUNT(1) AS receive_item_num
FROM
(SELECT product_label_no,receiving_scan_time FROM ods.wms_product_label_subject WHERE to_date(receiving_scan_time)='${hivevar:day}') t1
JOIN
(SELECT product_label_no,shop_id FROM dw.dim_wms_product_label) t2
ON t1.product_label_no=t2.product_label_no
GROUP BY shop_id) t3
ON t1.shop_id=t3.shop_id 

LEFT JOIN
(SELECT
shop_id,COUNT(DISTINCT product_label_no) AS enallot_item_num
FROM 
(SELECT
*,RANK() OVER(PARTITION BY product_label_no ORDER BY create_time DESC) AS rn
FROM
(SELECT  t1.*,t2.shop_id,t3.stop_print_time FROM 
(SELECT
*
FROM 
ods.wms__wms_product_label_log 
WHERE to_date(create_time) BETWEEN date_sub('${hivevar:day}',10) AND date_add('${hivevar:day}',1)) t1

JOIN
(SELECT product_label_no,shop_id FROM dw.dim_wms_product_label) t2
ON t1.product_label_no=t2.product_label_no

JOIN 
(SELECT shop_id,CASE WHEN stop_print_time='' THEN '17:00:00' ELSE stop_print_time END AS stop_print_time FROM ods.vvic__cooperate_shop) t3
ON t2.shop_id=t3.shop_id) temp
WHERE create_time<CONCAT_WS(' ','${hivevar:day}',stop_print_time)) temp
WHERE (rn=1 and current_life_status in (0,1,2,3)) or (current_life_status in (0,1,2) and to_date(create_time)='${hivevar:day}')
GROUP BY shop_id) t4
ON t1.shop_id=t4.shop_id ) temp WHERE enallot_item_num+allot_item_num+receive_item_num!=0;


INSERT OVERWRITE TABLE rpt.rpt_cooperate_daily PARTITION(`date`='${hivevar:day}') 
SELECT
SUM(CASE WHEN print_type=1 THEN 1 ELSE 0 END) AS send_shop_num,
SUM(CASE WHEN print_type=1 THEN enallot_item_num ELSE 0 END ) AS send_enallot_item_num,
SUM(CASE WHEN print_type=1 THEN allot_item_num ELSE 0 END) AS send_allot_item_num,
SUM(CASE WHEN print_type=2 THEN 1 ELSE 0 END) AS print_shop_num,
SUM(CASE WHEN print_type=2 THEN enallot_item_num ELSE 0 END ) AS print_enallot_item_num,
SUM(CASE WHEN print_type=2 THEN allot_item_num ELSE 0 END) AS print_allot_item_num,
SUM(receive_item_num) AS receive_item_num,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS insert_time
FROM
rpt.rpt_cooperate_shop_daily3
WHERE `date`='${hivevar:day}'
;
