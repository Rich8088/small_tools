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
from File_read import read_csv_file_to_dataframe, read_excel_file_to_dataframe

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
DB_URI = "mysql+pymysql://{}:{}@{}:{}/{}?charset=UTF8MB4". \
    format(ACCOUNT, PASSWORD, HOSTNAME, PORT, DATABASE)
engine = create_engine(DB_URI, pool_recycle=3600)
conn = engine.connect()  # --连接数据库
Base = declarative_base()  # --基类

# --------------------1、提取订单中有用信息-------------------------#
select_orders = 'SELECT 订单号,子订单号,子订单状态,拍下时间,付款时间,子订单发货时间,交易结束时间,' \
                '商品数字ID,SKUID,数量,淘宝单价,实际单价,实付金额,运费,实际收到金额,买家使用积分,退款状态, ' \
                '退款货物状态,买家是否需要退货,退款申请时间,退款金额,退款阶段,退款原因,优惠分摊,优惠详情,' \
                '买家是否评价,省,市,下载时间 FROM order_info'
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

# --------------------3、提取支付宝账单中有用信息-------------------------#
select_zhifubao = 'SELECT 业务流水号,商户订单号,发生时间,对方账号,收入金额,支出金额,业务类型,备注 FROM order_zhifubao'
df_select_zhifubao = pd.read_sql_query(sql=select_zhifubao, con=conn, coerce_float=True,
                                       parse_dates=None)
df_select_zhifubao.rename(columns={'发生时间': '交易时间'}, inplace=True)
df_select_zhifubao.sort_values(by=['业务流水号', '交易时间'], ascending=True, inplace=True)  # 数值排序
df_select_zhifubao = df_select_zhifubao.reset_index(drop=True)
df_last_zhifubao = df_select_zhifubao.drop_duplicates(subset=['业务流水号'], keep='last')
# print(df_last_zhifubao.info())

# ------------------4、读取优惠信息
df_coupon_info = read_csv_file_to_dataframe(rootDir, 'coupon', missing_values, 0, coding='utf-8')
# --汇总
df_grouped = pd.merge(df_last_orders, df_last_taoke, on="子订单号", how="left")
df_grouped = pd.merge(df_grouped, df_last_taoke_refund, on="子订单号", how="left")
df_grouped = pd.merge(df_grouped, df_coupon_info, on="订单号", how="left")
df_grouped[['退款金额', '维权退款金额', '应退回服务费', '应退回佣金']] = df_grouped[
    ['退款金额', '维权退款金额', '应退回服务费', '应退回佣金']].fillna(0)
df_grouped['淘客成交金额'] = df_grouped['实际成交价格'] - df_grouped['维权退款金额']
df_grouped['淘客佣金'] = df_grouped['佣金'] - df_grouped['应退回佣金']
df_grouped['淘客服务费'] = df_grouped['服务费'] - df_grouped['应退回服务费']
df_grouped['标识'] = df_grouped['商品ID']+df_grouped['SKUID']
df_grouped['是否付款'] = df_grouped['付款时间'].isnull()
df_grouped.drop(['下载时间', '维权完成时间', '实际成交价格', '维权退款金额', '佣金', '应退回佣金', '优惠分摊',
                 '优惠信息', '服务费', '应退回服务费', '路径', '文件名', '退款申请时间', '退款阶段',
                 '买家是否需要退货', '买家实际支付积分', '计划名称', '标价', '运费', '买家实际支付总金额'],
                axis=1, inplace=True)
df_grouped['拍下时间'] = df_grouped['拍下时间'].dt.date
df_grouped['付款时间'] = df_grouped['付款时间'].dt.date
df_grouped['子订单发货时间'] = df_grouped['子订单发货时间'].dt.date
df_grouped['交易结束时间'] = df_grouped['交易结束时间'].dt.date
df_grouped['淘客结算时间'] = df_grouped['淘客结算时间'].dt.date
print(df_grouped.info())
df_grouped_new = df_grouped.loc[df_grouped['活动分组'].isnull(), ['订单号', '优惠详情']]
# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/File/result/订单汇总.xlsx')
df_select_zhifubao.to_excel(writer, sheet_name='支付宝', header=True, index=False)
df_grouped_new.to_excel(writer, sheet_name='新订单', header=True, index=False)
# df_last_taoke.to_excel(writer, sheet_name='淘客', header=True, index=False)
# df_last_taoke_refund.to_excel(writer, sheet_name='淘客退款', header=True, index=False)
# df_last_orders.to_excel(writer, sheet_name='订单', header=True, index=False)
df_grouped.to_excel(writer, sheet_name='汇总', header=True, index=False)
writer.save()

end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
