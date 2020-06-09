# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
import pandas as pd
from time import time
import datetime
# --加载其他模块
from File_read import read_csv_file_to_dataframe

# --参数设置
start_time = time()
missing_values = ['n/a', 'na', '--', 'Null', 'NULL', '\t']
rootDir = '/home/rich/File/Original'  # 输入根目录的路径
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
df_orders['总件数'] = df_orders['总件数'].astype(float)
df_orders['优惠金额'] = df_orders['优惠金额'].astype(float)
df_orders['优惠分摊'] = df_orders['优惠分摊'].astype(float)
df_orders['退款金额'] = df_orders['退款金额'].astype(float)
df_orders['分阶段付款已付金额'] = df_orders['分阶段付款已付金额'].astype(float)
df_orders['满返红包'] = df_orders['满返红包'].astype(float)
df_orders['买家使用积分'] = df_orders['买家使用积分'].astype(float)
df_orders['实际收到金额'] = df_orders['实际收到金额'].astype(float)
df_orders['货到付款服务费'] = df_orders['货到付款服务费'].astype(float)
df_orders['信用卡支付金额'] = df_orders['信用卡支付金额'].astype(float)
df_orders['运费'] = df_orders['运费'].astype(float)
df_orders['实付金额'] = df_orders['实付金额'].astype(float)
df_orders['实际单价'] = df_orders['实际单价'].astype(float)
df_orders['淘宝单价'] = df_orders['淘宝单价'].astype(float)
df_orders['单价'] = df_orders['单价'].astype(float)
df_orders['价格'] = df_orders['价格'].astype(float)
df_orders['总价'] = df_orders['总价'].astype(float)
# 保留最新的数据记录
df_orders.sort_values(by=['订单号', '子订单号', '下载时间', '主图链接'], ascending=True, inplace=True)
df_orders = df_orders.reset_index(drop=True)
df_orders = df_orders.drop_duplicates(subset=['子订单号'], keep='last')
df_orders.drop(['index', '文件名', '路径', '店铺名', '下载时间'], axis=1, inplace=True)  # 删除不需要的列
print(df_orders.info())

# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/File/result/订单导出.xlsx', options={'strings_to_urls': False})
df_orders.to_excel(writer, sheet_name='汇总', header=True, index=False)
writer.save()

end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
