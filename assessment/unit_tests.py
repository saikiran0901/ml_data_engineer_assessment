# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:57:09 2022

@author: sravula
"""

import pytest
import pandas as pd


from profasee_etl_library.etl_library import TransformData


test_df = pd.DataFrame(data={"string_col":[" profasee","  amazon e-commerce "],"Phone_Number":["812-202-3251","+1-465-750-2170x566"]})

def test_clean_data_on_strings():
    transform = TransformData(test_df,"file","csv")
    assert transform.clean_string_data(test_df).loc[0]["string_col"] == 'profasee'
    assert transform.clean_string_data(test_df).loc[1]["string_col"] == 'amazon e-commerce'

def test_clean_phone_numbers():
    
    transform = TransformData(test_df,"file","csv")
    
    clean_number,extension,area_code,exchange_code,subscriber_number = transform.clean_phone_number_data("+1-465-750-2170x566")
    assert clean_number == "4657502170"
    assert extension == "566"
    assert area_code == "465"
    assert exchange_code == "750"
    assert subscriber_number == "2170"

if __name__ == '__main__':
   test_clean_data_on_strings()
   print("Unit Testing for cleaning string data is passed")
   test_clean_phone_numbers()
   print("Unit Testing for cleaning phone number data is passed")