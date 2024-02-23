import pandas as pd

"""It will call the GAM file and read"""
def extract_file():
    csv_path = 'C:/Users/anurag.sharma/Desktop/Project/GAM.csv'
    df= pd.read_csv(csv_path,low_memory=False)
    return df
