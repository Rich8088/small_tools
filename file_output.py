# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
import pandas as pd
import numpy as np
from time import time
from datetime import datetime
from sqlalchemy import create_engine  # 引擎
from sqlalchemy.ext.declarative import declarative_base  # 基类
from sqlalchemy import Column  # sqlalchemy类型
# mysql类型
from sqlalchemy.dialects.mysql import BIGINT, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, DOUBLE, \
    FLOAT, INTEGER, MEDIUMINT, SMALLINT, TIME, TIMESTAMP, TINYINT, VARCHAR, YEAR
from File_read import read_csv_file_to_dataframe

# --参数设置
start_time = time()
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 500)
missing_values = ['n/a', 'na', '--', 'Null', 'NULL']
rootDir = '/home/rich/File/Original'  # 输入根目录的路径

# --sqlalchemy基本操作
HOSTNAME = "127.0.0.1"
PORT = "3306"
DATABASE = "order"
ACCOUNT = "root"
PASSWORD = "123456"
DB_URI = "mysql+pymysql://{}:{}@{}:{}/{}?charset=UTF8MB4". \
    format(ACCOUNT, PASSWORD, HOSTNAME, PORT, DATABASE)
engine = create_engine(DB_URI, pool_recycle=3600)
conn = engine.connect()  # --连接数据库
Base = declarative_base()  # --基类

# --------------------1、提取订单中有用信息-------------------------#
select_orders = 'SELECT * FROM order_info'
df_orders = pd.read_sql_query(sql=select_orders, con=conn, coerce_float=True,
                              parse_dates=None)
df_orders.sort_values(by=['订单号', '子订单号', '下载时间'], ascending=True, inplace=True)  # 数值排序
df_select_orders = df_orders.reset_index(drop=True)
df_last_orders = df_select_orders.drop_duplicates(subset=['子订单号'], keep='last')
df_last_orders.drop(['下载时间', '卖家备注旗帜'], axis=1, inplace=True)
print(df_last_orders.info())

# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/File/result/订单汇总.xlsx')
df_last_orders.to_excel(writer, sheet_name='订单汇总', header=True, index=False)
writer.save()
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
