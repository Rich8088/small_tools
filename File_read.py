# -*- coding:utf-8 -*-
# -----------------------------------0、加载模块及初始化参数---------------------------------------#
# --加载常规模块
import os  # 文件路径
import pandas as pd


# --读取csv文件，根据关键字进行筛选，将选中的数据放入一个dataframe
def read_csv_file_to_dataframe(file_dir, file_name_key, missing_values, header_rows, coding):
    df_csv = pd.DataFrame()
    for parent, DirNames, FileNames in os.walk(file_dir):
        for FileName in FileNames:
            if FileName.__contains__('.csv'):
                if FileName.__contains__(file_name_key):
                    csv1 = pd.read_csv(os.path.join(parent, FileName), sep=',', index_col=None,
                                       encoding=coding,
                                       dtype=object, header=header_rows, na_values=missing_values)
                    csv1['路径'] = parent
                    csv1['文件名'] = FileName
                    df_csv = df_csv.append(csv1)
                else:
                    continue
            else:
                continue
    df_csv.dropna(axis=0, how='all', inplace=True)
    df_csv.drop_duplicates(keep='first', inplace=True)
    return df_csv

