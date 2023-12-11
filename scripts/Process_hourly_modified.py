import pandas as pd
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt
import argparse
from data_preprocessing import *

def process_and_save_csv(input_path, output_folder):
    print("Files::",input_path)
    df = pd.read_csv(input_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hourly_timestamp'] = df['timestamp'].dt.floor('10T')
    print(df)
    hourly_grouped_df = df.groupby(['campaign','batch','hourly_timestamp']).mean().reset_index()
    campaign_folder = os.path.join(output_folder, f'Campaign_{hourly_grouped_df["campaign"].iloc[0]}')
    os.makedirs(campaign_folder, exist_ok=True)
    output_path = os.path.join(campaign_folder, f'hourly_data_{os.path.basename(input_path)}')
    column_to_drop_index = 0  # Change this to the index of the column you want to drop
    print("hello::",df.columns[column_to_drop_index])
# Drop the column by index
    hourly_grouped_df = hourly_grouped_df.drop(df.columns[column_to_drop_index], axis=1)
    hourly_grouped_df.drop('timestamp',axis=1,inplace=True)
    hourly_grouped_df.to_csv(output_path, index=False)

    print(f"Processed and saved hourly data for Campaign {hourly_grouped_df['campaign'].iloc[0]} to {output_path}")

def process_all_files(input_folder, output_folder):
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            input_path = os.path.join(input_folder, filename)
            print(input_path)
            process_and_save_csv(input_path, output_folder)

# Example usage
source_folder = "C:/Users/kames/OneDrive/Desktop/capstone_main/data/Raw/Process/Standard"
destination_folder = 'C:/Users/kames/OneDrive/Desktop/capstone_main/data/Hourly_Standard/Process'

# process_all_files(source_folder, destination_folder)
folder_path = 'C:/Users/kames/OneDrive/Desktop/capstone_main/data/Hourly_Standard/Process'
dest_path = 'C:/Users/kames/OneDrive/Desktop/capstone_main/data/Hourly_Standard/Cleaned_Process'


process_csv_files_in_folders(folder_path,dest_path)
process_all_files(source_folder, destination_folder)

