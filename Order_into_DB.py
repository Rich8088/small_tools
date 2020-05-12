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
df_orders = read_csv_file_to_dataframe(rootDir, 'xlsx', missing_values, 0, coding='utf-8')
df_orders = df_orders.rename(columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
df_orders.dropna(axis=0, subset=['订单号', '子订单号'], inplace=True)
df_orders.replace('获取失败', None, inplace=True)
# --提取文件路径和下载时间信息
df_orders.reset_index(level=0, inplace=True)
df_path = df_orders['路径'].str.split('/', expand=True)
df_path.reset_index(level=0, inplace=True)
df_path.drop([0, 1, 2, 3, 4, 5], axis=1, inplace=True)
df_path.rename(columns={6: '店铺名'}, inplace=True)
df_orders = pd.merge(df_orders, df_path, how='left', on='index')
df_orders['下载时间'] = datetime.datetime.now().strftime('%Y') + df_orders['文件名'].str[12:20]
df_orders['下载时间'] = pd.to_datetime(df_orders['下载时间'], format='%Y%m%d%H%M')
# --字段处理
df_orders.drop(['主图链接', '宝贝链接', '卖家是否评价', 'index', '文件名', '路径', '预售订单最晚发货日期'],
               axis=1, inplace=True)  # 删除不需要的列
df_orders = df_orders[
    ['店铺名', '订单号', '子订单号', '订单状态', '子订单状态', '拍下时间', '付款时间', '发货时间', '子订单发货时间',
     '交易结束时间', '商品数字ID', 'SKUID', '属性', '商家编码', '宝贝名称', '数量', '省', '市', '区', '快递单号',
     '子订单运单号', '总价', '价格', '单价', '淘宝单价', '实际单价', '实付金额', '运费', '实际收到金额',
     '买家使用积分', '满返红包', '分阶段付款订单状态', '分阶段付款已付金额', '花呗分期期数', '退款状态', '退款货物状态',
     '退款申请时间', '退款更新时间', '退款金额', '退款阶段', '退款原因', '买家是否需要退货', '退货运单号', '优惠分摊',
     '优惠金额', '优惠详情', '买家留言', '卖家备注', '卖家备注旗帜', '买家是否评价', '下载时间']]
df_orders.sort_values(by=['订单号', '子订单号', '下载时间'], ascending=True, inplace=True)  # 数值排序
# 格式转化
df_orders['拍下时间'] = pd.to_datetime(df_orders['拍下时间'])
df_orders['付款时间'] = pd.to_datetime(df_orders['付款时间'])
df_orders['发货时间'] = pd.to_datetime(df_orders['发货时间'])
df_orders['交易结束时间'] = pd.to_datetime(df_orders['交易结束时间'])
df_orders['退款申请时间'] = pd.to_datetime(df_orders['退款申请时间'])
df_orders['退款更新时间'] = pd.to_datetime(df_orders['退款更新时间'])
df_orders['子订单发货时间'] = pd.to_datetime(df_orders['子订单发货时间'])
df_orders['数量'] = df_orders['数量'].astype(int)
df_orders['花呗分期期数'] = df_orders['花呗分期期数'].astype(int)
df_orders['优惠金额'] = df_orders['优惠金额'].astype(float)
df_orders['优惠分摊'] = df_orders['优惠分摊'].astype(float)
df_orders['退款金额'] = df_orders['退款金额'].astype(float)
df_orders['分阶段付款已付金额'] = df_orders['分阶段付款已付金额'].astype(float)
df_orders['满返红包'] = df_orders['满返红包'].astype(float)
df_orders['买家使用积分'] = df_orders['买家使用积分'].astype(float)
df_orders['实际收到金额'] = df_orders['实际收到金额'].astype(float)
df_orders['运费'] = df_orders['运费'].astype(float)
df_orders['实付金额'] = df_orders['实付金额'].astype(float)
df_orders['实际单价'] = df_orders['实际单价'].astype(float)
df_orders['淘宝单价'] = df_orders['淘宝单价'].astype(float)
df_orders['单价'] = df_orders['单价'].astype(float)
df_orders['价格'] = df_orders['价格'].astype(float)
df_orders['总价'] = df_orders['总价'].astype(float)
df_orders = df_orders.reset_index(drop=True)
df_last_orders = df_orders.drop_duplicates(subset=['子订单号'], keep='last')
# print(df_last_orders.info())
# --将原始数据存入数据库
type_dict_order = mapping_df_types(df_orders)
df_last_orders.to_sql(name='order_info', con=engine, if_exists='append', index=False,
                      dtype=type_dict_order)

print(df_last_orders.info())
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
