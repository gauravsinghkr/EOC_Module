
import pandas as pd
import cx_Oracle as OraCx

def connection():
    """connection on TFR"""
    connect = OraCx.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
    return connect

def check_print():
    query = 'Select * from TFR_Placement_Dim where IO_ID = 616907'
    output = pd.read_sql(query, connection())
    print(output)


check_print()



