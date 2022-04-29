# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:57:09 2022

@author: sravula
"""
import logging
import pandas as pd


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
    pass