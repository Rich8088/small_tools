# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
import pandas as pd
from time import time
import datetime
# --加载其他模块
from File_read import read_csv_file_to_dataframe
from sqlalchemy.dialects.mysql import BIGINT, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, DOUBLE, \
    FLOAT, INTEGER, MEDIUMINT, SMALLINT, TIME, TIMESTAMP, TINYINT, VARCHAR, YEAR  # mysql类型
from sqlalchemy import create_engine  # 引擎
from sqlalchemy.ext.declarative import declarative_base  # 基类

# --参数设置
start_time = time()
pd.set_option('display.max_rows', 20)
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
DB_URI = "mysql+pymysql://{}:{}@{}:{}/{}?charset=UTF8MB4" \
    .format(ACCOUNT, PASSWORD, HOSTNAME, PORT, DATABASE)
engine = create_engine(DB_URI, pool_recycle=3600)
conn = engine.connect()  # --连接数据库
Base = declarative_base()  # --基类


# --数据库类型字典
def mapping_df_types(df):
    type_dict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            type_dict.update({i: VARCHAR(length=255)})
        if "float" in str(j):
            type_dict.update({i: FLOAT(precision=2, asdecimal=True)})
        if "int" in str(j):
            type_dict.update({i: INTEGER()})
    return type_dict


# --------------------1、读取原始订单数据文件并存入数据库--------#

# --读取订单原始数据,并输入数据库
df_zhifubao = read_csv_file_to_dataframe(rootDir, '账务明细', missing_values, 4, coding='gbk')
df_zhifubao = df_zhifubao.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
df_zhifubao.dropna(axis=0, subset=['业务流水号', '发生时间'], inplace=True)
# --提取文件路径和下载时间信息
df_zhifubao.reset_index(level=0, inplace=True)
df_path = df_zhifubao['路径'].str.split('/', expand=True)
df_path.reset_index(level=0, inplace=True)
df_path.drop([0, 1, 2, 3, 4, 5], axis=1, inplace=True)
# --字段处理
df_zhifubao.rename(
    columns={'收入金额（+元）': '收入金额', '支出金额（-元）': '支出金额', '账户余额（元）': '账户余额'},
    inplace=True)
df_zhifubao.drop(['账务流水号', '商品名称', 'index', '文件名', '路径', '交易渠道'], axis=1, inplace=True)  # 删除不需要的列
df_zhifubao.sort_values(by=['业务流水号', '发生时间'], ascending=True, inplace=True)  # 数值排序

# 格式转化
df_zhifubao['发生时间'] = pd.to_datetime(df_zhifubao['发生时间'])
df_zhifubao['收入金额'] = df_zhifubao['收入金额'].astype(float)
df_zhifubao['支出金额'] = df_zhifubao['支出金额'].astype(float)
df_zhifubao['账户余额'] = df_zhifubao['账户余额'].astype(float)
df_zhifubao = df_zhifubao.reset_index(drop=True)
df_last_zhifubao = df_zhifubao.drop_duplicates(subset=['业务流水号'], keep='last')
# --将原始数据存入数据库
type_dict_zhifubao = mapping_df_types(df_last_zhifubao)
df_last_zhifubao.to_sql(name='order_zhifubao', con=engine, if_exists='append', index=False,
                        dtype=type_dict_zhifubao)

end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
