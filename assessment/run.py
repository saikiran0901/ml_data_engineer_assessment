# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:57:09 2022

@author: sravula
"""
# Importing ETL functions from custom built library

from profasee_etl_library.etl_library import TransformData
from profasee_etl_library.etl_library import LoadData
from profasee_etl_library.db_utils import config
import pandas as pd
import os
import logging

# Setting the input and output directories
input_dir = config.INPUT_DATA_DIR
output_dir = config.OUTPUT_DATA_DIR
process_files = ['people.csv']

def convert_raw_data_to_json():
    print("converting raw data to json")
    file_name='people.csv'
    input_data = pd.read_csv(os.path.join(input_dir,file_name),sep=',',low_memory=False)
    input_data.to_json(os.path.join(output_dir,f"{file_name.split('.')[0]}.json"))

def clean_the_files():

    # Multiple Cleaning needs to be done before feeding to the ML Team

    # 1) Strings have trailing and leading spaces which needs to be trimmed, there might be duplicates due to spaces
    # 2) Phone Numbers are not in standard format, have to clean them to get the area code, 
    # exchange code which can be used to group people belonging to the same area
    # 3) Fill NA values with some average median or mean values either by backfill or forwardfill
    # 4) In the interests column, few values are similar, we can try to find the nearest values and club them together

    print("Cleaning the csv file and storing in data folder")
    
    for file_name in process_files:
      if file_name.split('.')[-1] == 'csv':
       data_source = 'file'
       file_format = file_name.split('.')[-1] 
       input_data = pd.read_csv(os.path.join(input_dir,file_name),sep=',',low_memory=False)
       etl_process = TransformData(input_data,data_source,file_format) # Call the ETL Class
       final_data = etl_process.perform_etl()
       final_data.to_csv(os.path.join(output_dir,'dim_people.csv'),index=False)       

def get_people_with_no_interests():

    # Getting the people with no interests from the csv file
    print("Getting people with no interests from the csv file")    

    input_data = pd.read_csv(os.path.join(input_dir,process_files[0]),sep=',',low_memory=False)
    etl_process = TransformData(input_data,'file','csv') # Call the ETL Class
    final_data = etl_process.perform_etl()
    filtered_data = final_data[final_data["Interest1"].isnull()&final_data["Interest2"].isnull()&final_data["Interest3"].isnull()&final_data["Interest4"].isnull()]
    filtered_data.to_csv(os.path.join(output_dir,"people_with_no_interest.csv"),index=False)
    
def load_csv_to_mysql():
    print("Loading cleaned data from csv to the mysql database")
    input_data = pd.read_csv(os.path.join(input_dir,process_files[0]),sep=',',low_memory=False)
    etl_process = TransformData(input_data,'file','csv') # Call the Transform ETL Class
    final_data = etl_process.perform_etl()  # Perform cleaning on the dataset
    load_data = LoadData() # Load the Loading ETL Class 

    load_data.load_df_to_table(final_data,'dim_people','Y') # Insert the transformed data into the table
    print("Inserted csv people data into dim_people table")    

def get_max_min_avg_age():
    print("Getting Max,Min and Avg age")
    load_data = LoadData()
    with open(os.path.join('sql_queries','get_max_min_avg_age.sql')) as f:
        sql_query = f.read()
    results=load_data.run_sql(sql_query,'Y')
    print("Minimum Age",results['min_age'][0])
    print("Maximum Age",results['max_age'][0])
    print("Average Age",results['avg_age'][0])
    return f"Minimum Age: {results['min_age'][0]}, Maximum Age:{results['max_age'][0]}, Average Age:{results['avg_age'][0]}  "

def get_city_with_most_people():
    print("Getting city with most people")
    load_data = LoadData()

    with open(os.path.join('sql_queries','get_city_with_most_people.sql')) as f:
        sql_query = f.read()    
    results=load_data.run_sql(sql_query,'Y')
    print("City with Maximum people is",results['city'][0])
    return f"City with most people: {results['city'][0]}"

def get_top_5_common_interest():
    print("getting Top 5 common Interests")
    load_data = LoadData()

    with open(os.path.join('sql_queries','get_top_5_common_interest.sql')) as f:
        sql_query = f.read()    
    results=load_data.run_sql(sql_query,'Y')
    print("Top 5 common Interests",results)  
    results_common_interest = ','.join([row['interest'] for i,row in results.iterrows()])
    return f"Top 5 common Interests: {results_common_interest}"

if __name__ == "__main__":
   try: 
    convert_raw_data_to_json() 
    clean_the_files()  
    get_people_with_no_interests()
    load_csv_to_mysql()   
    get_max_min_avg_age()
    get_city_with_most_people()
    get_top_5_common_interest()
   except Exception as e:
    print(e)    
