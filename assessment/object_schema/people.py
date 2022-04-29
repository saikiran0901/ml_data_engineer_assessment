# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:57:09 2022

@author: sravula
"""
import logging
import pandas as pd

class ETL():
    
    def __init__(self,raw_data,datasource,format):
        
        if datasource == 'file' and format == 'csv':
            self.raw_data = raw_data
            self.data_source = 'csv'

        else:
            pass
    
    def perform_etl(self):
        
        if self.data_source == 'csv':
            
          self.pre_process_csv()
          
          self.clean_string_data()           
          
          return self.transform_data
        
    def clean_string_data(self):
        self.transform_data = self.transform_data.select_dtypes(['object'])
        self.transform_data[self.transform_data.columns] = self.transform_data.apply(lambda x: x.str.strip())
    
    def clean_numeric_data(self):
        pass
    
            
    def pre_process_csv(self):
        
        self.transform_data = self.raw_data.fillna('')   # Fill null values with None
        
        self.transform_data = self.transform_data.dropna(how='all') # Drop rows which are having null values
        
        self.transform_data.drop_duplicates(subset=None,keep='first',inplace=True,ignore_index=True) # Drop duplicates
