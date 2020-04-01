# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
import pandas as pd
from time import time
import datetime
# --加载其他模块
from File_read import read_csv_file_to_dataframe, read_excel_file_to_dataframe
from sqlalchemy import Column  # sqlalchemy类型
# mysql类型
from sqlalchemy.dialects.mysql import BIGINT, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, DOUBLE, \
    FLOAT, INTEGER, MEDIUMINT, SMALLINT, TIME, TIMESTAMP, TINYINT, VARCHAR, YEAR
from sqlalchemy import create_engine  # 引擎
from sqlalchemy.ext.declarative import declarative_base  # 基类

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


# --------------------1、读取原始数据文件并存入原始数据库--------#

# --读取订单原始数据,并输入数据库
df_orders = read_csv_file_to_dataframe(rootDir, 'ExportOrderList', missing_values, 0)
df_orders = df_orders.rename(columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
df_orders.dropna(axis=0, subset=['订单编号', '支付单号'], inplace=True)
df_orders.drop(['文件名', '宝贝标题'], axis=1, inplace=True)
type_dict_order = mapping_df_types(df_orders)
df_orders.to_sql(name='order_info', con=engine, if_exists='append', index=False,
                 dtype=type_dict_order)
# print(df_orders.info())

# --读取订单sku原始数据,并输入数据库
df_sku = read_csv_file_to_dataframe(rootDir, 'ExportOrderDetailList', missing_values, 0)
df_sku = df_sku.rename(columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
df_sku.dropna(axis=0, subset=['订单编号', '商家编码'], inplace=True)
df_sku.drop(['文件名', '标题'], axis=1, inplace=True)
type_dict_sku = mapping_df_types(df_sku)
df_sku.to_sql(name='order_sku', con=engine, if_exists='append', index=False, dtype=type_dict_sku)
# print(df_sku.info())

# --------------------2、提取订单中有用信息，生成提取库-------------------------#
# --读取原始库，并进行处理
select_order = 'SELECT 订单编号,订单创建时间,订单付款时间,确认收货时间, 买家应付货款,买家应付邮费,' \
               '总金额,退款金额,买家实际支付金额,天猫卡券抵扣,宝贝种类,宝贝总数量,买家支付宝账号,' \
               '订单关闭原因,订单状态,收货地址 FROM order_info'
df_select_order = pd.read_sql_query(sql=select_order, con=conn, coerce_float=True, parse_dates=None)
df_select_order = pd.concat(
    [df_select_order, df_select_order['收货地址'].str.split(' ', expand=True)[[0, 1]]], axis=1)
df_select_order.rename(
    columns={'订单编号': 'order_id', '订单创建时间': 'create_time', '订单付款时间': 'pay_time',
             '确认收货时间': 'delivery_time', '买家应付货款': 'payment_goods', '买家应付邮费': 'payment_postage',
             '总金额': 'payment_order', '买家实际支付金额': 'payment_consumer', '天猫卡券抵扣': 'payment_coupon',
             '退款金额': 'refund', '订单关闭原因': 'order_ClosingReason', '订单状态': 'order_status',
             '宝贝总数量': 'total_quantity_goods', '宝贝种类': 'type_goods', '买家支付宝账号': 'consumer_account',
             0: 'region_first', 1: 'region_second'},
    inplace=True)
df_select_order.drop(['收货地址'], axis=1, inplace=True)
df_select_order['create_time'] = pd.to_datetime(df_select_order['create_time'])
df_select_order['pay_time'] = pd.to_datetime(df_select_order['pay_time'])
df_select_order['delivery_time'] = pd.to_datetime(df_select_order['delivery_time'])
df_select_order['payment_goods'] = df_select_order['payment_goods'].astype(float)
df_select_order['payment_postage'] = df_select_order['payment_postage'].astype(float)
df_select_order['payment_order'] = df_select_order['payment_order'].astype(float)
df_select_order['payment_consumer'] = df_select_order['payment_consumer'].astype(float)
df_select_order['payment_coupon'] = df_select_order['payment_coupon'].astype(float)
df_select_order['refund'] = df_select_order['refund'].astype(float)
df_select_order['payment_coupon'] = df_select_order['payment_coupon'].astype(float)
df_select_order['type_goods'] = df_select_order['type_goods'].astype(int)
df_select_order['total_quantity_goods'] = df_select_order['total_quantity_goods'].astype(int)


# --定义表
class Order(Base):
    __tablename__ = 'orders_pick'
    id_ord = Column(BIGINT(unsigned=True), primary_key=True, comment='主键:自增序号')
    order_id = Column(CHAR(20), nullable=False, index=True, comment='订单编号')
    create_time = Column(TIMESTAMP(), comment='订单创建时间')
    pay_time = Column(TIMESTAMP(), index=True, comment='订单付款时间')
    delivery_time = Column(TIMESTAMP(), comment='确认收货时间')
    payment_goods = Column(DECIMAL(9, 2), comment='买家应付货款')
    payment_postage = Column(DECIMAL(9, 2), comment='买家应付邮费')
    payment_order = Column(DECIMAL(9, 2), comment='订单金额')
    payment_consumer = Column(DECIMAL(9, 2), comment='买家实际付款')
    payment_coupon = Column(DECIMAL(9, 2), comment='天猫卡券抵扣')
    refund = Column(DECIMAL(9, 2), comment='退款金额')
    type_goods = Column(TINYINT(), comment='商品种类')
    total_quantity_goods = Column(SMALLINT(), comment='商品总数量')
    consumer_account = Column(VARCHAR(50), comment='买家支付宝账号')
    order_ClosingReason = Column(VARCHAR(50), comment='订单关闭原因')
    order_status = Column(VARCHAR(50), comment='订单状态')
    region_first = Column(VARCHAR(50), comment='地址:省/直辖市')
    region_second = Column(VARCHAR(50), comment='地址:地级市/区')


# --将表写入数据库
Base.metadata.create_all(engine)
# --插入数据
df_select_order.to_sql('orders_pick', con=engine, if_exists='append', index=False)

# --保留最新数据
del_order_pick = 'delete from orders_pick where id_ord in(' \
                 'SELECT UNUSED_ID.id_ord from(' \
                 'SELECT t3.id_ord FROM orders_pick t3 WHERE t3.id_ord not in (' \
                 'SELECT t1.id_ord FROM orders_pick t1 LEFT JOIN orders_pick t2 ' \
                 'ON t1.order_id = t2.order_id AND t1.id_ord < t2.id_ord ' \
                 'WHERE t2.id_ord IS NULL)) AS UNUSED_ID )'
query_del_order_pick = pd.read_sql_query(sql=del_order_pick, con=conn, coerce_float=True,
                                         parse_dates=None, chunksize=1)

# -----------------------------------3、提取sku中有用信息，生成提取库-----------------------------------#
# --读取原始库，并进行处理
select_sku = 'SELECT 订单编号,购买数量,外部系统编号,商品属性,商家编码,订单状态 FROM order_sku'
df_select_sku = pd.read_sql_query(sql=select_sku, con=conn, coerce_float=True, parse_dates=None)
df_select_sku.rename(
    columns={'订单编号': 'order_id', '外部系统编号': 'External_Code', '商品属性': 'Commodity_Attribute',
             '商家编码': 'Merchant_Encoding', '购买数量': 'quantity_goods', '订单状态': 'order_status'},
    inplace=True)
df_select_sku['quantity_goods'] = df_select_sku['quantity_goods'].astype(int)


# --定义表
class SKU(Base):
    __tablename__ = 'sku_pick'
    id_sku = Column(BIGINT(unsigned=True), primary_key=True, comment='主键:自增序号')
    order_id = Column(CHAR(20), nullable=False, index=True, comment='订单编号')
    quantity_goods = Column(SMALLINT(), comment='购买数量')
    External_Code = Column(VARCHAR(50), comment='外部系统编号')
    Commodity_Attribute = Column(VARCHAR(100), comment='商品属性')
    Merchant_Encoding = Column(VARCHAR(50), comment='商家编码')
    order_status = Column(VARCHAR(50), comment='订单状态')


# --将表写入数据库
Base.metadata.create_all(engine)
# --插入数据
df_select_sku.to_sql('sku_pick', con=engine, if_exists='append', index=False)
# --保留最新数据
delete_sku = 'delete from sku_pick where id_sku in(' \
             'SELECT UNUSED.id_sku from (' \
             'SELECT t3.id_sku FROM sku_pick t3 WHERE t3.id_sku not in (' \
             'SELECT t1.id_sku FROM sku_pick t1 LEFT JOIN sku_pick t2 ' \
             'ON t1.Merchant_Encoding = t2.Merchant_Encoding AND t1.order_id = t2.order_id ' \
             'AND t1.id_sku < t2.id_sku WHERE t2.id_sku IS NULL)) AS UNUSED) '
query_delete_order_sku = pd.read_sql_query(sql=delete_sku, con=conn, coerce_float=True,
                                           parse_dates=None, chunksize=1)
# --根据关联关系，删除多余的数据
delete_un = 'delete from sku_pick where id_sku in(' \
            'SELECT UNUSED.id_sku from (' \
            'SELECT t1.id_sku FROM sku_pick t1 LEFT JOIN orders_pick t2 ' \
            'ON t1.order_id = t2.order_id WHERE t2.order_id IS NULL )AS UNUSED)'
query_delete_u = pd.read_sql_query(sql=delete_un, con=conn, coerce_float=True, parse_dates=None,
                                   chunksize=1)

end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
