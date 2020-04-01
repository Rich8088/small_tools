# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
import pandas as pd
import numpy as np
from time import time
import datetime
from sqlalchemy import create_engine  # 引擎
from sqlalchemy.ext.declarative import declarative_base  # 基类
from sqlalchemy import Column  # sqlalchemy类型
# mysql类型
from sqlalchemy.dialects.mysql import BIGINT, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, DOUBLE, \
    FLOAT, INTEGER, MEDIUMINT, SMALLINT, TIME, TIMESTAMP, TINYINT, VARCHAR, YEAR
from File_read import read_csv_file_to_dataframe, read_excel_file_to_dataframe

# --参数设置
start_time = time()
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 500)
missing_values = ['n/a', 'na', '--', 'Null', 'NULL']
rootDir = '/home/rich/MyFile/order/ini'  # 输入根目录的路径

# --sqlalchemy基本操作
HOSTNAME = "127.0.0.1"
PORT = "3306"
DATABASE = "test"
USERNAME = "root"
PASSWORD = "123456"
DB_URI = "mysql+pymysql://{}:{}@{}:{}/{}?charset=UTF8MB4".format(USERNAME, PASSWORD, HOSTNAME, PORT,
                                                                 DATABASE)
engine = create_engine(DB_URI, pool_recycle=3600)
conn = engine.connect()  # --连接数据库
Base = declarative_base()  # --基类


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


# ---提取sku信息,存入数据库-----#
df_product_info = read_excel_file_to_dataframe(rootDir, 'sku信息', missing_values, 0)
df_product_info = df_product_info.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
df_product_info.dropna(axis=0, subset=['商家编码', 'sku简称'], inplace=True)
df_product_info.drop(['文件名'], axis=1, inplace=True)
df_product_info['更新时间'] = datetime.datetime.now()
df_product_info['更新时间'] = pd.to_datetime(df_product_info['更新时间'])
df_product_info.rename(
    columns={'商家编码': 'Merchant_Encoding', 'sku简称': 'sku_name', '参考价格': 'reference_price',
             '更新时间': 'update_time'},
    inplace=True)
# print(df_product_info)
type_dict_order = mapping_df_types(df_product_info)
df_product_info.to_sql(name='product_info', con=engine, if_exists='append', index=False,
                       dtype=type_dict_order)
# --保留最新数据
delete_product_info = 'delete from product_info where update_time in(' \
                      'SELECT UNUSED_ID.update_time from(' \
                      'SELECT t3.update_time FROM product_info t3 WHERE t3.update_time not in (' \
                      'SELECT t1.update_time FROM product_info t1 LEFT JOIN product_info t2 ' \
                      'ON t1.Merchant_Encoding = t2.Merchant_Encoding ' \
                      'AND t1.update_time < t2.update_time ' \
                      'WHERE t2.Merchant_Encoding IS NULL)) AS UNUSED_ID )'
query_delete_product_info = pd.read_sql_query(sql=delete_product_info, con=conn, coerce_float=True,
                                              parse_dates=None, chunksize=1)

# --提取需要的数据，分别为订单，sku，价格表
select_order = 'SELECT * FROM orders_pick WHERE create_time is not null'
df_select_order = pd.read_sql_query(sql=select_order, con=conn, coerce_float=True, parse_dates=None)

select_sku = 'SELECT * FROM sku_pick '
df_select_sku = pd.read_sql_query(sql=select_sku, con=conn, coerce_float=True, parse_dates=None)

select_price = 'SELECT * FROM product_info '
df_select_price = pd.read_sql_query(sql=select_price, con=conn, coerce_float=True, parse_dates=None)
df_select_price['reference_price'] = df_select_price['reference_price'].astype(float)
# --生成中间临时表，先对sku数据进行分组汇总，然后形成两个表的交叉汇总
grouped = df_select_sku.groupby('order_id')
df_grouped = pd.merge(grouped.aggregate({'quantity_goods': np.sum}),
                      grouped["Merchant_Encoding"].count(), on="order_id", how="inner")
df_grouped.reset_index(level=0, inplace=True)
df_grouped.rename(columns={'Merchant_Encoding': 'sku总类别'}, inplace=True)
df_middle_check = pd.merge(
    df_grouped, df_select_order[
        ['order_id', 'payment_order', 'payment_consumer', 'type_goods', 'total_quantity_goods']],
    on="order_id", how="inner")
df_middle_sku = pd.merge(
    df_select_sku[['order_id', 'quantity_goods', 'Merchant_Encoding', 'order_status']],
    df_select_order[
        ['order_id', 'pay_time', 'payment_order', 'type_goods', 'region_first', 'region_second']],
    on="order_id", how="inner")

# --提取只包含一种商品的订单，计算当前销售均价
df_middle_sku1 = df_middle_sku.loc[df_middle_sku['type_goods'] == 1]
df_middle_sku1 = df_middle_sku1.set_index(["Merchant_Encoding"])
grouped_sku1 = df_middle_sku1[['payment_order', 'quantity_goods']].groupby('Merchant_Encoding')
sum_sku1 = grouped_sku1.sum()
sum_sku1['均价'] = sum_sku1['payment_order'] / sum_sku1['quantity_goods']
sum_sku1['均价'] = np.round(sum_sku1['均价'], 1)
sum_sku1.reset_index(level=0, inplace=True)
df_price = pd.merge(
    df_select_price[['reference_price', 'Merchant_Encoding']],
    sum_sku1[['均价', 'Merchant_Encoding']], on="Merchant_Encoding", how="left")
df_price['均价'].fillna(df_price['reference_price'], inplace=True)
df_middle_sku1.reset_index(level=0, inplace=True)

# --提取包含多个商品的订单，根据预估均价
df_middle_sku2 = df_middle_sku.loc[df_middle_sku['type_goods'] > 1]
temp = pd.merge(df_middle_sku2, df_price[['Merchant_Encoding', '均价']], on="Merchant_Encoding",
                how="left")
temp = temp.set_index(["order_id"])
grouped_temp = temp[['均价']].groupby('order_id')
sum_temp = grouped_temp.sum()
sum_temp.reset_index(level=0, inplace=True)
df_middle_sku2 = pd.merge(df_middle_sku2, sum_temp[['均价', 'order_id']], on="order_id", how="left")
df_middle_sku2 = pd.merge(df_middle_sku2, df_price[['均价', 'Merchant_Encoding']],
                          on="Merchant_Encoding", how="left")
df_middle_sku2['payment_order'] = (df_middle_sku2['payment_order'] * df_middle_sku2['均价_y']) / \
                                  df_middle_sku2['均价_x']
df_middle_sku2['payment_order'] = np.round(df_middle_sku2['payment_order'], 3)
df_middle_sku2.drop(['均价_x', '均价_y'], axis=1, inplace=True)
df_result = pd.concat([df_middle_sku1, df_middle_sku2], axis=0)
df_result = pd.merge(df_result, df_select_price[['Merchant_Encoding', 'sku_name']],
                     on="Merchant_Encoding", how="left")
# --统计汇总
df_result_sum = df_result[["sku_name", "pay_time", 'quantity_goods', 'payment_order']]
df_result_sum = df_result_sum.set_index(["pay_time"])
df_result_sum['pay_date'] = df_result_sum.index.to_period("D")
df_result_sum = df_result_sum.set_index(["pay_date", "sku_name"])
grouped_result_sum = df_result_sum.groupby(["pay_date", "sku_name"]).sum()
grouped_result_sum.reset_index(inplace=True)

# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/MyFile/order/results/excel.xlsx')
df_price.to_excel(writer, sheet_name='商品价格', header=True, index=False)
df_result.to_excel(writer, sheet_name='sku销售数据', header=True, index=False)
grouped_result_sum.to_excel(writer, sheet_name='sku销售汇总', header=True, index=False)
writer.save()
# print(df_result.info())
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
