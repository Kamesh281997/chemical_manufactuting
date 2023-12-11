import pandas as pd
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt
import argparse
from sklearn.preprocessing import StandardScaler


def drop_nulls_below_threshold(dataframe, threshold=3):
    null_count = dataframe.isnull().sum()
    null_percentage = (null_count / len(dataframe)) * 100
    columns_to_drop = null_percentage[null_percentage < threshold].index
    dataframe = dataframe.dropna(subset=columns_to_drop)
    return dataframe



def scale_columns(dataframe, columns_to_scale):
    df_scaled = dataframe.copy()
    scaler = StandardScaler()
    for column in columns_to_scale:
        column_data = df_scaled[column].values.reshape(-1, 1)
        scaled_column_data = scaler.fit_transform(column_data)
        df_scaled[column] = scaled_column_data.flatten()

    return df_scaled


def drop_columns(dataframe, columns_to_drop):
    df_dropped = dataframe.copy()
    df_dropped = df_dropped.drop(columns=columns_to_drop, errors='ignore')
    return df_dropped


def process_csv_files_in_folders(root_folder,dest_path):
    if not os.path.exists(root_folder):
        raise FileNotFoundError(f"The specified root folder '{root_folder}' does not exist.")
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    processed_data = {}

    for folder_path, _, files in os.walk(root_folder):
        for filename in files:
            print(filename)
            if filename.endswith(".csv"):
                file_path = os.path.join(folder_path, filename)
                df = pd.read_csv(file_path)
                df_after_drop = drop_columns(df, ['code', 'cyl_pre'])
                cleaned_dataframe = drop_nulls_below_threshold(df_after_drop)
                scaled_dataframe = scale_columns(cleaned_dataframe, ['produced','fom','ejection', 'waste','tbl_speed'])
                dest=os.path.join(dest_path,filename)
                scaled_dataframe.to_csv(dest)
            else:
                print("All Files Data transfromed")










