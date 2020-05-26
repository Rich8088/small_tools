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
df_taoke = read_csv_file_to_dataframe(rootDir, 'settle', missing_values, 0, coding='gbk')
df_taoke = df_taoke.rename(columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
df_taoke.dropna(axis=0, subset=['淘客结算时间', '来源或淘客昵称'], inplace=True)
# --提取文件路径和下载时间信息
df_taoke.reset_index(level=0, inplace=True)
df_path = df_taoke['路径'].str.split('/', expand=True)
df_path.reset_index(level=0, inplace=True)
df_path.drop([0, 1, 2, 3, 4, 5], axis=1, inplace=True)
df_path.rename(columns={6: '店铺名'}, inplace=True)
df_taoke = pd.merge(df_taoke, df_path, how='left', on='index')
# --字段处理
df_taoke.drop(['商品名称', '服务费率', 'index', '文件名', '路径', '佣金比例'], axis=1, inplace=True)  # 删除不需要的列
df_taoke = df_taoke[
    ['店铺名', '来源或淘客昵称', '团长名称', '计划名称', '创建时间', '确认收货时间', '淘客结算时间',
     '淘宝父订单编号', '淘宝子订单编号', '实际成交价格', '成交商品数', '佣金', '服务费金额']]
df_taoke.sort_values(by=['淘宝父订单编号', '淘宝子订单编号', '创建时间'], ascending=True, inplace=True)  # 数值排序
# 格式转化
df_taoke['创建时间'] = pd.to_datetime(df_taoke['创建时间'])
df_taoke['确认收货时间'] = pd.to_datetime(df_taoke['确认收货时间'])
df_taoke['淘客结算时间'] = pd.to_datetime(df_taoke['淘客结算时间'])
df_taoke['成交商品数'] = df_taoke['成交商品数'].astype(int)
df_taoke['实际成交价格'] = df_taoke['实际成交价格'].astype(float)
df_taoke['佣金'] = df_taoke['佣金'].astype(float)
df_taoke['服务费金额'] = df_taoke['服务费金额'].astype(float)
df_taoke = df_taoke.reset_index(drop=True)
df_last_taoke = df_taoke.drop_duplicates(subset=['淘宝子订单编号'], keep='last')
# --将原始数据存入数据库
type_dict_order = mapping_df_types(df_last_taoke)
df_last_taoke.to_sql(name='order_taoke', con=engine, if_exists='append', index=False,
                     dtype=type_dict_order)

# --读取订单原始数据,并输入数据库
df_taoke_refund = read_csv_file_to_dataframe(rootDir, 'Refund', missing_values, 0, coding='utf-8')
df_taoke_refund = df_taoke_refund.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
df_taoke_refund.dropna(axis=0, subset=['订单结算时间', '淘宝子订单编号'], inplace=True)
# --提取文件路径和下载时间信息
df_taoke_refund.reset_index(level=0, inplace=True)
df_path_refund = df_taoke_refund['路径'].str.split('/', expand=True)
df_path_refund.reset_index(level=0, inplace=True)
df_path_refund.drop([0, 1, 2, 3, 4, 5], axis=1, inplace=True)
df_path_refund.rename(columns={6: '店铺名'}, inplace=True)
df_taoke_refund = pd.merge(df_taoke_refund, df_path_refund, how='left', on='index')
# --字段处理
df_taoke_refund.drop(['淘宝订单编号', '商品名称', 'index', '文件名', '路径', '维权创建时间'], axis=1,
                     inplace=True)  # 删除不需要的列
df_taoke_refund = df_taoke_refund[
    ['店铺名', '淘宝子订单编号', '维权状态', '订单结算时间', '维权完成时间', '维权退款金额', '应退回服务费', '应退回佣金']]
df_taoke_refund.sort_values(by=['淘宝子订单编号', '维权完成时间'], ascending=True, inplace=True)  # 数值排序
# 格式转化
df_taoke_refund['订单结算时间'] = pd.to_datetime(df_taoke_refund['订单结算时间'])
df_taoke_refund['维权完成时间'] = pd.to_datetime(df_taoke_refund['维权完成时间'])
df_taoke_refund['应退回服务费'] = df_taoke_refund['应退回服务费'].astype(float)
df_taoke_refund['维权退款金额'] = df_taoke_refund['维权退款金额'].astype(float)
df_taoke_refund['应退回佣金'] = df_taoke_refund['应退回佣金'].astype(float)
df_taoke_refund = df_taoke_refund.reset_index(drop=True)
df_last_taoke_refund = df_taoke_refund.drop_duplicates(subset=['淘宝子订单编号'], keep='last')
# --将原始数据存入数据库
type_dict_taoke_refund = mapping_df_types(df_last_taoke_refund)
df_last_taoke_refund.to_sql(name='order_taoke_refund', con=engine, if_exists='append', index=False,
                            dtype=type_dict_taoke_refund)
print(df_last_taoke)
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
