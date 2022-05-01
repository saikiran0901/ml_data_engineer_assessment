# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:57:09 2022

@author: sravula
"""

from profasee_etl_library.etl_library import TransformData
from profasee_etl_library.etl_library import LoadData
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
    
def load_csv_to_mysql():
    input_data = pd.read_csv(os.path.join(input_dir,process_files[0]),sep=',',low_memory=False)
    etl_process = TransformData(input_data,'file','csv') # Call the ETL Class
    final_data = etl_process.perform_etl()  
    load_data = LoadData()
    #load_data.run_sql("insert into dim_people(name) values('Manasa');",'N')
    
    #final_data.to_sql()
    load_data.load_df_to_table(final_data,'dim_people_stg','Y')
    print("inserted_data_into_table")
    load_data.run_sql("select count(1) from dim_people limit 10",'Y')
    print("count before load")
    load_data.run_sql("""merge dim_people tgt using dim_people_stg src on
    tgt.name = src.name
when matched then 
     SET 
	 tgt.Age = src.Age ,
	 tgt.City = src.City,
	 tgt.Interest1 = src.Interest1,
	 tgt.Interest2 = src.Interest2,
	 tgt.Interest3 = src.Interest3,
	 tgt.Interest4 = src.Interest4,
	 tgt.PhoneNumber = src.PhoneNumber,
	 tgt.w_update_timestamp = current_timestamp
WHEN NOT MATCHED THEN
	 INSERT(Age,City,Interest1,Interest2,Interest3,Interest4,PhoneNumber,w_create_timestamp,w_update_timestamp)
	 VALUES(src.Age,src.City,src.Interest1,src.Interest2,src.Interest3,src.Interest4,src.PhoneNumber,current_timestamp,current_timestamp)
WHEN NOT MATCHED BY SOURCE
THEN DELETE	  """,'N')
    load_data.run_sql("select count(1) from dim_people limit 10",'Y')
    print("count after load")
    print("selected_data_from_table")

if __name__ == "__main__":
    load_csv_to_mysql()
