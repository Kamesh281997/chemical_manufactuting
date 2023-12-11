from main import connect
import pandas as pd
import numpy as np
import psycopg2
from config import config
import matplotlib.pyplot as plt
import os
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller


def get_data(extrt):
    df=connect(extrt)
    df.to_csv("../data/all_combined.csv")
    df['hourly_timestamp'] = pd.to_datetime(df['hourly_timestamp'])
    df.set_index('hourly_timestamp',inplace=True)
    stacked_df=df.copy()
    stacked_df.drop(["waste"],axis=1,inplace=True)
    lst=stacked_df.columns
    plot_graph(df,lst)
    all_contribution(df)
    decomposition(df)
    column_seasonality(df)
    adfuller_test(df)
    
    
def plot_graph(df,lst):
    # df['hourly_timestamp'] = pd.to_datetime(df['hourly_timestamp'])
    output_directory = '../graph/batch_line_charts'
    os.makedirs(output_directory, exist_ok=True)
    for batch_number, batch_df in df.groupby('batch'):
        for val in lst:
            plt.figure(figsize=(12, 6))
            plt.plot(batch_df.index, batch_df[val], label='Tablet Speed', marker='o')
            plt.plot(batch_df.index, batch_df['waste'], label='Waste', marker='o')
            plt.title(f'{val} and Waste Over Time for Batch {batch_number}')
            plt.xlabel('Hourly Timestamp')
            plt.ylabel('Value')
            plt.legend()
            plt.grid(True)
            
            output_file_path = os.path.join(output_directory, f'v{val}_batch_{batch_number}_line_chart.png')
            plt.savefig(output_file_path)
            plt.close()

    print("Line charts saved.")
   
   
def all_contribution(stacked_data):
    dataset = stacked_data
    output_directory = '../graph/all_col_distributions'
    os.makedirs(output_directory, exist_ok=True)
    columns_to_plot = ['tbl_speed', 'fom', 'main_comp', 'tbl_fill', 'srel', 'produced', 'waste', 'ejection']
    batch_groups = dataset.groupby('batch')

    for batch_number, batch_data in batch_groups:
        plt.figure(figsize=(12, 6))
        for i, column in enumerate(columns_to_plot, start=1):
            plt.subplot(len(columns_to_plot), 1, i)
            sns.histplot(batch_data[column], bins=20, kde=True, color='skyblue', edgecolor='black')  # Adjust the number of bins as needed
            plt.title(f'Batch {batch_number} - {column}')
        output_file_path = os.path.join(output_directory, f'batch_{batch_number}_plot.png')
        plt.savefig(output_file_path)
        plt.tight_layout()
        # plt.show()

def decomposition(df):        
    
    columns_to_decompose = [  'tbl_speed', 'fom', 'main_comp',
           'tbl_fill', 'srel', 'produced', 'waste', 'cyl_main',
            'stiffness', 'ejection']  

    batch_groups = df.groupby('batch')

    season_directory = '../graph/seasons'
    os.makedirs(season_directory, exist_ok=True)
    trends_directory = '../graph/trends'
    os.makedirs(trends_directory, exist_ok=True)
    observed_directory = '../graph/observed'
    os.makedirs(observed_directory, exist_ok=True)
    resid_directory = '../graph/resid'
    os.makedirs(resid_directory, exist_ok=True)

    for batch_number, batch_data in batch_groups:
        for column in columns_to_decompose:
            result = seasonal_decompose(batch_data[column], model='additive', period=2) 
            plt.figure(figsize=(12, 8))
            plt.subplot(4, 1, 1)
            plt.plot(result.observed, label='Observed')
            plt.legend()
            output_file_path = os.path.join(observed_directory, f'batch_{batch_number}_{column}_plot.png')
            plt.savefig(output_file_path)

            plt.subplot(4, 1, 2)
            plt.plot(result.trend, label='Trend')
            plt.legend()
            output_file_path1 = os.path.join(trends_directory, f'batch_{batch_number}_{column}_plot.png')
            plt.savefig(output_file_path1)
            plt.subplot(4, 1, 3)
            plt.plot(result.seasonal, label='Seasonal')
            plt.legend()
            output_file_path2 = os.path.join(season_directory, f'batch_{batch_number}_{column}_plot.png')
            plt.savefig(output_file_path2)      

            plt.subplot(4, 1, 4)
            plt.plot(result.resid, label='Residuals')
            plt.legend()
            output_file_path4 = os.path.join(resid_directory, f'batch_{batch_number}_{column}_plot.png')
            plt.savefig(output_file_path4)

            plt.suptitle(f'Batch {batch_number} - {column} Time Series Decomposition')
            plt.tight_layout()
            # plt.show()

def column_seasonality(df):
    batch_numbers = df['batch'].unique()
    output_directory = '../graph/columns_distribution'
    os.makedirs(output_directory, exist_ok=True)
    for batch_number in batch_numbers:
        batch_data = df[df['batch'] == batch_number]
        for feature in df.columns.difference(['batch']):
            plt.figure(figsize=(12, 6))
            plt.plot(batch_data.index, batch_data[feature], label=feature)
            plt.title(f'Batch {batch_number} - {feature} Over Time')
            plt.xlabel('Timestamp')
            plt.ylabel(feature)
            plt.legend()

            output_file_path = os.path.join(output_directory, f'batch_{batch_number}_{feature}_plot.png')
            plt.savefig(output_file_path)
            plt.close() 

    print("Line charts saved successfully.")

def adfuller_test(stacked_data):
    
    batch_numbers = stacked_data['batch'].unique()
    features = stacked_data.columns.difference(['batch',  'campaign'])  # Exclude non-numeric columns
    batch_lst=[]
    feature_lst=[]
    dct={}
    for feature in features:
        print(f"Feature: {feature}")
        for batch_number in batch_numbers:
            batch_number_int64 = np.int64(batch_number)
            batch_data = stacked_data[(stacked_data['batch'] == batch_number)][feature]

            try:
                test_result = adfuller(batch_data)
                adf_statistic = test_result[0]
                p_value = test_result[1]
                critical_values = test_result[4]

                
                # print(f'Batch {batch_number_int64} ADF Statistic: {adf_statistic}')
                # print(f'Batch {batch_number_int64} p-value: {p_value}')
                # print(f'Batch {batch_number_int64} Critical Values:')
                # for key, value in critical_values.items():
                #     print(f'   {key}: {value}')

                if p_value <= 0.05:
                    
                    print(f"Batch {batch_number_int64}: Reject the null hypothesis - The data is stationary.")
                else:
                    batch_lst.append(batch_number)
                    feature_lst.append(feature)
                    # Use placeholders to avoid SQL injection
                    print(f"Batch {batch_number_int64}: Fail to reject the null hypothesis - The data is non-stationary.")
                print()
    
            except Exception as e:
                print(f"Error processing Batch {batch_number_int64} for Feature {feature}: {e}")
        df =pd.DataFrame({"Batch_number":batch_lst,"Feature":feature_lst})
        df.to_csv("../data/non_stationary.csv")
    
if __name__ == "__main__":
    get_data("extract")