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
select_orders = 'SELECT 订单号,子订单号,子订单状态,拍下时间,付款时间,子订单发货时间,交易结束时间,省,市,' \
                '商品数字ID,SKUID,数量,淘宝单价,实际单价,实付金额,运费,实际收到金额,买家使用积分, ' \
                '退款申请时间,退款更新时间,退款金额,退款原因,优惠详情,买家是否评价,下载时间 FROM order_info'
df_select_orders = pd.read_sql_query(sql=select_orders, con=conn, coerce_float=True,
                                     parse_dates=None)
df_select_orders.rename(
    columns={'商品数字ID': '商品ID', '数量': '购买数量', '淘宝单价': '标价', '实际单价': '购买价格',
             '实付金额': '应付货款', '实际收到金额': '买家实际支付总金额', '买家使用积分': '买家实际支付积分'},
    inplace=True)
df_select_orders.sort_values(by=['订单号', '子订单号', '下载时间'], ascending=True, inplace=True)  # 数值排序
df_select_orders = df_select_orders.reset_index(drop=True)
df_last_orders = df_select_orders.drop_duplicates(subset=['子订单号'], keep='last')
# print(df_last_orders.info())

# --------------------2、提取淘客中有用信息-------------------------#
select_taoke = 'SELECT 来源或淘客昵称,团长名称,计划名称,淘客结算时间,淘宝子订单编号,实际成交价格,佣金,' \
               '服务费金额 FROM order_taoke'
df_select_taoke = pd.read_sql_query(sql=select_taoke, con=conn, coerce_float=True, parse_dates=None)
df_select_taoke.rename(columns={'来源或淘客昵称': '淘客昵称', '服务费金额': '服务费', '淘宝子订单编号': '子订单号',
                                '团长名称': '团长'},
                       inplace=True)
df_select_taoke.sort_values(by=['子订单号', '淘客结算时间'], ascending=True, inplace=True)  # 数值排序
df_select_taoke = df_select_taoke.reset_index(drop=True)
df_last_taoke = df_select_taoke.drop_duplicates(subset=['子订单号'], keep='last')
# print(df_last_taoke.info())

# --------------------3、提取淘客维权中有用信息-------------------------#
select_taoke_refund = 'SELECT 维权退款金额,应退回服务费,应退回佣金,维权完成时间,淘宝子订单编号 FROM order_taoke_refund'
df_select_taoke_refund = pd.read_sql_query(sql=select_taoke_refund, con=conn, coerce_float=True,
                                           parse_dates=None)
df_select_taoke_refund.rename(columns={'淘宝子订单编号': '子订单号'}, inplace=True)
df_select_taoke_refund.sort_values(by=['子订单号', '维权完成时间'], ascending=True, inplace=True)  # 数值排序
df_select_taoke_refund = df_select_taoke_refund.reset_index(drop=True)
df_last_taoke_refund = df_select_taoke_refund.drop_duplicates(subset=['子订单号'], keep='last')
# print(df_last_taoke_refund.info())

# ------------------3、读取优惠信息
df_coupon_info = read_csv_file_to_dataframe(rootDir, 'coupon', missing_values, 0, coding='utf-8')
df_coupon_info['购物券金额'] = df_coupon_info['购物券金额'].astype(float)
df_coupon_info.drop(['路径', '文件名'], axis=1, inplace=True)
# print(df_coupon_info.info())
df_taoke_info = read_csv_file_to_dataframe(rootDir, 'taoke_info', missing_values, 0, coding='utf-8')
df_taoke_info.drop(['路径', '文件名'], axis=1, inplace=True)
# print(df_taoke_info.info())

# ------------------4、读取地理信息
df_geo_info = read_csv_file_to_dataframe(rootDir, 'geo_city', missing_values, 0, coding='utf-8')
df_geo_info['标识b'] = df_geo_info['省份'] + df_geo_info['城市']
df_geo_info.drop(['路径', '文件名', '省编号', '行政区域', '省份', '城市', 'tableau城市'], axis=1, inplace=True)
df_geo_un = read_csv_file_to_dataframe(rootDir, 'city_un', missing_values, 0, coding='utf-8')
df_coupon_info['购物券金额'] = df_coupon_info['购物券金额'].astype(float)
df_geo_un.drop(['路径', '文件名', '省份', '省管市'], axis=1, inplace=True)
# ------------------5、读取商品信息
df_product = read_csv_file_to_dataframe(rootDir, 'product', missing_values, 0, coding='utf-8')
df_product['标识c'] = df_product['商品ID'] + df_product['SKU ID']
df_product.drop(['路径', '文件名', '商品ID', 'SKU ID', '商品SKU'], axis=1, inplace=True)
# print(df_product.info())

# --汇总
df_grouped = pd.merge(df_last_orders, df_last_taoke, on="子订单号", how="left")
df_grouped = pd.merge(df_grouped, df_last_taoke_refund, on="子订单号", how="left")
df_grouped = pd.merge(df_grouped, df_coupon_info, on="订单号", how="left")
df_grouped = pd.merge(df_grouped, df_taoke_info, on="淘客昵称", how="left")
df_grouped['是否付款'] = df_grouped['付款时间'].notnull()
df_grouped['是否淘客'] = df_grouped['淘客结算时间'].notnull()
df_grouped['是否发货'] = df_grouped['子订单发货时间'].notnull()
df_grouped['是否退款'] = df_grouped['退款金额'].notnull()
df_grouped['是否完结'] = df_grouped['交易结束时间'].notnull()
df_grouped.replace(True, "是", inplace=True)
df_grouped.replace(False, "否", inplace=True)
fna_values = {'退款金额': 0, '实际成交价格': 0, '佣金': 0, '服务费': 0, '维权退款金额': 0,
              '应退回服务费': 0, '应退回佣金': 0, '优惠金额': 0, 'SKUID': '-'}
df_grouped_order = df_grouped.fillna(value=fna_values)
df_grouped_order['淘客成交金额'] = df_grouped_order['实际成交价格'] - df_grouped_order['维权退款金额']
df_grouped_order['淘客佣金'] = df_grouped_order['佣金'] - df_grouped_order['应退回佣金']
df_grouped_order['淘客服务费'] = df_grouped_order['服务费'] - df_grouped_order['应退回服务费']
df_grouped_order.drop(['下载时间', '维权完成时间', '实际成交价格', '维权退款金额', '佣金', '应退回佣金', '运费',
                       '服务费', '应退回服务费', '买家实际支付积分', '计划名称', '标价',
                       '买家实际支付总金额'], axis=1, inplace=True)
df_grouped_order['淘客结算时间'] = df_grouped_order['淘客结算时间'].dt.date
df_order_new = df_grouped_order.loc[df_grouped_order['优惠分组'].isnull(),
                                    ['订单号', '拍下时间', '优惠详情', '淘客昵称', '团长']]
# print(df_grouped_order.info())
# --地理信息处理
df_grouped_order['标识a'] = df_grouped_order['省'] + df_grouped_order['市']
df_grouped_order = pd.merge(df_grouped_order, df_geo_un, on="标识a", how="left")
df1 = pd.DataFrame(df_grouped_order.loc[df_grouped_order['对应省'].isnull()])
df0 = pd.DataFrame(df_grouped_order.loc[df_grouped_order['对应省'].notnull()])
df1.drop(['对应省', '对应市'], axis=1, inplace=True)
df0.drop(['省', '市'], axis=1, inplace=True)
df0.rename(columns={'对应省': '省', '对应市': '市'}, inplace=True)
df_order = pd.concat([df1, df0], axis=0, ignore_index=True)
df_order['标识b'] = df_order['省'] + df_order['市']
df_order = pd.merge(df_order, df_geo_info, on="标识b", how="left")
df_order['人口'] = df_order['人口'].astype(float)
df_order['纬度'] = df_order['纬度'].astype(float)
df_order['经度'] = df_order['经度'].astype(float)
# print(df_order.info())
# --商品信息处理
df_order['标识c'] = df_order['商品ID'] + df_order['SKUID']
df_order = pd.merge(df_order, df_product, on="标识c", how="left")
df_order_gn = df_order.loc[df_order['省份简称'].isnull(), ['省', '市']]
df_order_pn = df_order.loc[df_order['商品分组'].isnull(), ['商品ID', 'SKUID']]
df_order.drop(['标识a', '标识b', '标识c', '省', '市', '优惠详情', 'SKUID', '退款申请时间', '下单时间'], axis=1,
              inplace=True)
# print(df_order.info())
# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/File/result/订单汇总.xlsx')
df_order_new.to_excel(writer, sheet_name='新订单', header=True, index=False)
df_order_gn.to_excel(writer, sheet_name='新省市', header=True, index=False)
df_order_pn.to_excel(writer, sheet_name='新商品', header=True, index=False)
df_order.to_excel(writer, sheet_name='汇总', header=True, index=False)
writer.save()

end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
