# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:57:09 2022

@author: sravula
"""
import logging
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import re


# Defining class to handle all the Extracts
class ExtractData():
    pass

# Defining class to handle all the transformations
class TransformData():
    
    def __init__(self,raw_data,datasource,format):
        
        if datasource == 'file' and format == 'csv':
            self.raw_data = raw_data
            self.data_source = 'csv'

        else:
            pass
    
    # Performing a series of transformations on the input data
    def perform_etl(self):
        
        if self.data_source == 'csv':
            
          self.transform_data = self.pre_process_csv(self.raw_data)          
          self.transform_data = self.clean_string_data(self.transform_data)
          self.transform_data = self.clean_phone_number_df(self.transform_data,'PhoneNumber')
          return self.transform_data

    # Function to clean the string data by stripping off the leading and trailing whitespaces    
    def clean_string_data(self,data):
        data_filtered = data.select_dtypes(['object'])
        data[data_filtered.columns] = data_filtered.apply(lambda x: x.str.strip())        
        return data
    
    #Function to clean the phone number in a dataframe
    def clean_phone_number_df(self,df,column_name):              
        df['Phone_Country_Code'] = '+1'
        df[['Full_Phone_Number','Phone_Extension','Phone_Area_Code','Phone_Exchange_Code','Phone_Subscriber_Number']] = df.apply(lambda row : self.clean_phone_number_data(row[column_name]), axis = 1,result_type="expand")
        df.drop(column_name, inplace=True, axis=1)       
        return df

    #Function to clean the phone number
    def clean_phone_number_data(self,string):
        phonePattern = re.compile(r'(\d{3})\D*(\d{3})\D*(\d{4})\D*(\d*)$')
        groups=phonePattern.search(string).groups()
        clean_phone_number=''.join([groups[0],groups[1],groups[2]])
        if groups[3]:
            extension = groups[3]
        else:
            extension = ''    
        return clean_phone_number,extension,groups[0],groups[1],groups[2]
    
    # Preprocess a dataframe, removing nulls and duplicates        
    def pre_process_csv(self,data):
        
        #self.transform_data = data.fillna('')   # Fill null values in case required
        
        self.transform_data = data.dropna(how='all') # Drop rows which are having null values
        
        self.transform_data.drop_duplicates(subset=None,keep='first',inplace=True,ignore_index=True) # Drop duplicates
        return self.transform_data

# Class to load all the data to a datawarehouse or DataLake
class LoadData():
    
    def __init__(self):
        self.db='mysql'
        self.create_db_engine(self.db)

    def create_db_engine(self,db):
        if db == 'mysql':
          self.engine = sqlalchemy.create_engine("mysql://datatest:alligator@database/datatestdb")
          self.connection = self.engine.connect()   

    def run_sql(self,sql,get_results):
        sql = text(sql)
        
        df_results = pd.read_sql_query(sql, self.engine)
        if get_results == 'Y':
            pass
        return df_results    
   
    def load_df_to_table(self,df,table_name,truncate):
        if truncate == 'Y':
         results = self.engine.execute(f"TRUNCATE TABLE {table_name}") 
        df.to_sql(con=self.connection, name=table_name, if_exists='append',index=False)
