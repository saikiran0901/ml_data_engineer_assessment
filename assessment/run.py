# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:57:09 2022

@author: sravula
"""

from profasee_etl_library.etl_library import TransformData
from profasee_etl_library.db_utils import config
from mapping import mapping_io


import pandas as pd
import os
import logging

input_dir = config.INPUT_DATA_DIR
output_dir = config.OUTPUT_DATA_DIR
process_files = ['people.csv']

def run():
    print("Hello, Profasee!")
    
    for file_name in process_files:
     try:   
      if file_name.split('.')[-1] == 'csv':
       data_source = 'file'
       file_format = file_name.split('.')[-1] 
       input_data = pd.read_csv(os.path.join(input_dir,file_name),sep=',',low_memory=False)
       etl_process = TransformData(input_data,data_source,file_format) # Call the ETL Class
       final_data = etl_process.perform_etl()
       final_data.to_json(os.path.join(output_dir,f"{file_name.split('.')[0]}.json"))
       
       
     except Exception as e:
       print(e)


def get_people_with_no_interests():
    input_data = pd.read_csv(os.path.join(input_dir,process_files[0]),sep=',',low_memory=False)
    etl_process = TransformData(input_data,'file','csv') # Call the ETL Class
    final_data = etl_process.perform_etl()
    filtered_data = final_data[final_data["Interest1"].isnull()&final_data["Interest2"].isnull()&final_data["Interest3"].isnull()&final_data["Interest4"].isnull()]
    filtered_data.to_csv(os.path.join(output_dir,"people_with_no_interest.csv"),index=False)
    
    
if __name__ == "__main__":
    get_people_with_no_interests()
