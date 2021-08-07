# -*- coding: utf-8 -*-
from pyhive import hive
import MySQLdb as mdb
import datetime,traceback,sys
import json
reload(sys)
sys.setdefaultencoding( "utf-8" )

#执行改程序的方式
#python hive2mysql.py hive库名 hive表名 bi表名 需要抽取数据的日期
#python hive2mysql.py label ad_crm_tags ad_crm_tags_v2_copy1 2020-03-16


def hive2mysql(json_dir,stati_date):
    #配置文件的获取
    with open(json_dir, "r") as f:
        json_data = f.read()
    json_data=json.loads(json_data)
    #判断数据是从写入hdfs还是读取hdfs
    json_reader=json_data["job"]["content"][0]["reader"]
    json_writer = json_data["job"]["content"][0]["writer"]["parameter"]
    if json_reader["name"]=="hdfsreader" :
        db_name=json_reader["parameter"]["path"].split('.db/')[0].split('/')[-1]
        hive_ta=json_reader["parameter"]["path"].split('.db/')[1].split('/')[0]
        bi_ta=json_writer["connection"][0]["table"][0]
    # 数据库连接
    bi_conn = mdb.connect(host=json_writer["connection"][0]["jdbcUrl"].replace('jdbc:mysql://','').split(':')[0],
                          port=3306, user=json_writer["username"], passwd=json_writer["password"],
                          db='vvic-bi', charset='utf8')
    bi_engine = bi_conn.cursor()
    # hvie连接
    conn = hive.Connection(host='192.168.100.18', port=10000, username='root')
    hive_engine = conn.cursor()

    # 判断是否入库前需要做一些sql操作
    if 'preSql' in json_writer :
        try:
            bi_engine.execute(json_writer['preSql'][0].replace('$DATE',stati_date))
        except:
            traceback.print_exc()
            pass

    # 数据更新
    column=''
    sql_string = ''
    for column_li in  json_writer['column']:
        # 拼凑表字段信息
        column=column+','+column_li
        # 拼凑sql的插入字段长度
        sql_string = sql_string + ",%s"
    if json_reader['parameter']["path"].find('$DATE/*')>-1 :
        tsql = "select %s from %s.%s where `%s`='%s'" % (column[1:].replace(',date,',',`date`,').replace('date,','`date`,').replace(',date',',`date`'),db_name, hive_ta, json_reader['parameter']["path"].split('/')[-2].split('=')[0],stati_date)
    else:
        tsql = "select %s from %s.%s " % (column[1:].replace(',date,',',`date`,').replace(',date',',`date`').replace('date,','`date`,'), db_name, hive_ta)
    #print tsql
    try:
        # 获取数据
        hive_engine.execute(tsql)
        data = hive_engine.fetchall()
        # 拼凑sql
        tsql='insert into %s ('%bi_ta+column[1:]+') values('+sql_string[1:]+')'
        data_list=[]
        i=0
        for i in range(len(data)/100000):
            data_list=data[i*100000:(1+i)*100000]
            
            bi_engine.executemany(tsql,data_list)
            bi_conn.commit()
            data_list=[]
        if i>0:
            data_list=data[(i+1)*100000:]
        else:
            data_list=data
        bi_engine.executemany(tsql,data_list)
        bi_conn.commit()
        bi_engine.execute(json_writer['postSql'][0])
        bi_conn.commit()
    except:
        traceback.print_exc()
        pass

    #关闭数据库连接
    if bi_engine is not None:
        bi_engine.close()
    #关闭hive连接
    if hive_engine is not None:
        hive_engine.close()

if __name__ == '__main__':
    if len(sys.argv)==3:
        hive2mysql(sys.argv[1],sys.argv[2])
