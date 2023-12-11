import pandas as pd
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt
import argparse

def standardized_file(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                print(folder_path+"/"+file)
                path=os.path.join(folder_path,file)
                df = pd.read_csv(path, header=None, names=['Column'])
                unique_columns = df['Column'].unique()
                columns=unique_columns[0]
                column_name=columns.split(';')
                lst=[unique_columns[i].split(';') for i in range(1,len(unique_columns))]
                df2=pd.DataFrame(lst,columns=column_name)
                print("Number of unique batches are in process {}".format(df2["batch"].nunique()))
                directory_path=f"{folder_path}/Standard/"
                if not os.path.exists(directory_path):
                    os.makedirs(f"{folder_path}/Standard/")
                df2.to_csv(f"{folder_path}/Standard/{file}")
    print("Sum of all unique batches are {}".format(sum))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Input filename")
    parser.add_argument("argument", help="Filename Input:")
    args = parser.parse_args()
    standardized_file(args.argument)
    print(args.argument)    
    
    
    
    



