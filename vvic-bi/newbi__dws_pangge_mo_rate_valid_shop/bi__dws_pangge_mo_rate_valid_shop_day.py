# -*- coding: utf-8 -*- 

from sqlalchemy.engine import create_engine
from pyhive import hive
import pandas as pd

def bi__dws_pangge_mo_rate_valid_shop():
    hive_engine=hive.Connection(host='192.168.100.18', port=10000, username='root', database='dws')
    bi_engine=create_engine('mysql+pymysql://biz_i:1JL7FNMKI690LwHa@probi001.mysql.rds.aliyuncs.com:3306/vvic-bi')

    tsql = "truncate table dws_pangge_mo_rate_valid_shop"
    try:
        pd.read_sql_query(tsql,bi_engine)
    except:
        pass

    tsql = "select * from information_schema.tables where table_schema='vvic-bi' and table_type='dws_pangge_mo_rate_valid_shop'"
    try:
        data_df = pd.read_sql_query(tsql,bi_engine)
    except:
        pass

    if data_df.empty:
        tsql = "desc dws.dws_pangge_mo_rate_valid_shop_day"
        col_df = pd.read_sql(tsql, hive_engine)
        if col_df.empty:
            return
        col_df = col_df[col_df['data_type'].isin(['int', 'bigint', 'double', 'string'])]
        col_df = col_df.drop_duplicates(['col_name', 'data_type', 'comment'], keep='first')
        if col_df.empty:
            return

        create_list = []
        for index, row in col_df.iterrows():
            if row['data_type'] == 'bigint':
                d_type = "bigint(20)"
                s_name = "`%s`" % row['col_name'].lower()
            elif row['data_type'] == 'int':
                d_type = "int"
                s_name = "`%s`" % row['col_name'].lower()
            elif row['data_type'] == 'float':
                d_type = "float"
                s_name = "`%s`" % row['col_name'].lower()
            elif row['data_type'] == 'double':
                d_type = "double"
                s_name = "`%s`" % row['col_name'].lower()
            elif row['data_type'] == 'string' and row['col_name'].find('date')>=0:
                d_type = "varchar(10)"
                s_name = "`%s`" % row['col_name'].lower()
            elif row['data_type'] == 'string' and row['col_name'].find('time')>=0:
                d_type = "timestamp"
                s_name = "`%s`" % row['col_name'].lower()
            elif row['data_type'] == 'string':
                d_type = "varchar(255)"
                s_name = "`%s`" % row['col_name'].lower()
            else:
                return
            create_list.append("`%s` %s comment '%s'" % (row['col_name'].lower(), d_type, row['comment']))
        create_list.append("`%s` %s comment '%s'" % (row['col_name'].lower(), d_type, row['comment']))
        create_list.append("`insert_time` timestamp comment '写入时间' ")

        tsql = "create table `dws_pangge_mo_rate_valid_shop`( %s )" % (',\n'.join(create_list))
        try:
            pd.read_sql_query(tsql,bi_engine)
        except:
            pass

    tsql = "select * ,substr(current_timestamp, 0, 19) as insert_time from dws.dws_pangge_mo_rate_valid_shop_day where `date`=date_sub(current_date(),1)" 
    try:
        data_df = pd.read_sql_query(tsql,hive_engine)
        data_df.rename(columns=lambda x: x.replace('dws_pangge_mo_rate_valid_shop_day.', ''), inplace=True)
        data_df.to_sql('dws_pangge_mo_rate_valid_shop', bi_engine, index=False, if_exists='append')
    except:
        pass

if __name__ == '__main__':
    bi__dws_pangge_mo_rate_valid_shop()