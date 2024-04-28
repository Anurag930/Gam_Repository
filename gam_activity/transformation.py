import pandas as pd
import re
import numpy as np
from datetime import datetime
from gam_activity.loggers import logger


class Tranformation:
    def __init__(self, df):
        self.df = df
 
# Replace '-' with null values and convert NAN to null
    def replace_nan_with_null(self):
        self.df.replace('-', pd.NA, inplace=True)
        self.df.replace('NULL',pd.NA,inplace=True)
      
# covert date to standard datetime format()
    def conversion(self,date):
# if date is 11/02/2023 it will convert to 02/11/2023 
        if re.match(r'\d{2}-\d{2}-\d{4}', date):
            return datetime.strftime(datetime.strptime(date, '%m-%d-%Y').date(),'%d-%m-%Y')
# if date is 11/02/23 it will convert to 02/11/2023        
        elif re.match(r'\d{2}/\d{2}/\d{2}', date):
            return datetime.strftime(datetime.strptime(date, '%m/%d/%y').date(),'%d-%m-%Y')
        else:
            return None

# Creating date_id from date column      
    def process_date_column(self, date_column, date_id_column): 
        self.df[date_column] = pd.to_datetime(self.df[date_column], format='%d-%m-%Y') 
        self.df[date_id_column] = self.df[date_column].dt.strftime('%Y%m%d') 
        return self.df
    
    # creatung a hour_id_column       
    def add_hour_id_column(self, date_column, hour_id_column): 
        self.df[hour_id_column] = self.df[date_column].dt.hour 
        self.df[date_column] = self.df[date_column].dt.strftime('%d-%m-%Y') 
        return self.df
   
# creating a trafficker_column
    def process_trafficker_column(self, Trafficker, trafficker_name, trafficker_id):
        self.df[[trafficker_name, trafficker_id]] = self.df[Trafficker].str.split('(', expand=True)
        self.df[trafficker_id] = self.df[trafficker_id].str.rstrip(')')
        return self.df
    
# Converting the columns into snake_case
    def to_snake_case(self):
        self.df.columns = [col.replace(' ', '_').lower() for col in self.df.columns]
        return self.df
   
logger.info("Tranformation Done")       
