# -*- coding:utf-8 -*-
# -----------------------------------0、加载模块及初始化参数---------------------------------------#
# --加载常规模块
import os  # 文件路径
import pandas as pd


# --读取csv文件，根据关键字进行筛选，将选中的数据放入一个dataframe
def read_csv_file_to_dataframe(file_dir, file_name_key, missing_values, header_rows):
    df_csv = pd.DataFrame()
    for parent, DirNames, FileNames in os.walk(file_dir):
        for FileName in FileNames:
            if FileName.__contains__('.csv'):
                if FileName.__contains__(file_name_key):
                    csv1 = pd.read_csv(os.path.join(parent, FileName), sep=',', index_col=None,
                                       encoding='utf-8',
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


# --读取xls文件，根据关键字进行筛选，将选中的数据放入一个dataframe
def read_excel_file_to_dataframe(file_dir, sheet_name_key, missing_values, skip_rows):
    df_xls = pd.DataFrame()
    for parent, DirNames, FileNames in os.walk(file_dir):
        for FileName in FileNames:
            if FileName.__contains__('.xls'):
                xls_file = pd.ExcelFile(os.path.join(parent, FileName))
                sheet_names = xls_file.sheet_names  # 读取sheet名称
                for sht in sheet_names:
                    if sht.__contains__(sheet_name_key):
                        xls1 = pd.read_excel(os.path.join(parent, FileName), sheet_name=sht,
                                             skiprows=skip_rows,
                                             dtype=object, na_values=missing_values, thousands=',',
                                             encoding='utf8')
                        xls1['路径'] = parent
                        xls1['文件名'] = sht
                        df_xls = df_xls.append(xls1)
                    else:
                        continue
            else:
                continue
    df_xls.dropna(axis=0, how='all', inplace=True)
    df_xls.drop_duplicates(keep='first', inplace=True)
    return df_xls
