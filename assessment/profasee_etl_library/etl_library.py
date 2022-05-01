# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:57:09 2022

@author: sravula
"""
import logging
import pandas as pd
import sqlalchemy
from sqlalchemy import text

class ExtractData():
    pass

class TransformData():
    
    def __init__(self,raw_data,datasource,format):
        
        if datasource == 'file' and format == 'csv':
            self.raw_data = raw_data
            self.data_source = 'csv'

        else:
            pass
    
    def perform_etl(self):
        
        if self.data_source == 'csv':
            
          self.transform_data = self.pre_process_csv(self.raw_data)          
          self.transform_data = self.clean_string_data(self.transform_data)           
          
          return self.transform_data
        
    def clean_string_data(self,data):
        data = data.select_dtypes(['object'])
        data[data.columns] = data.apply(lambda x: x.str.strip())
        return data
    
    def clean_numeric_data(self):
        pass
    
            
    def pre_process_csv(self,data):
        
        #self.transform_data = data.fillna('')   # Fill null values with None
        
        self.transform_data = data.dropna(how='all') # Drop rows which are having null values
        
        self.transform_data.drop_duplicates(subset=None,keep='first',inplace=True,ignore_index=True) # Drop duplicates
        return self.transform_data

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
        results = self.engine.execute(sql)  
        
        if get_results == 'Y':
         # View the records
         for record in results:
            print("\n", record)

    def create_table(self,sql):
        pass
    
    def load_df_to_table(self,df,table_name,truncate):
        if truncate == 'Y':
         results = self.engine.execute(f"TRUNCATE TABLE {table_name}") 
        df.to_sql(con=self.connection, name=table_name, if_exists='append')

    def upsert_into_table(df,table):
        pass