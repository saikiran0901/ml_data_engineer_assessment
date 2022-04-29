# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:57:09 2022

@author: sravula
"""

import pytest
import pandas as pd


from profasee_etl_library.etl_library import TransformData


test_df = pd.DataFrame(data={"string_col":[" profasee","  amazon e-commerce "]})

def test_clean_data_on_strings():
    transform = TransformData(test_df,"file","csv")
    assert transform.clean_string_data(test_df).loc[0]["string_col"] == 'profasee'
    assert transform.clean_string_data(test_df).loc[1]["string_col"] == 'amazon e-commerce'