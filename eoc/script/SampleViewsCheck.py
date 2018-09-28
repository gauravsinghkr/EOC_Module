
import pandas as pd
import cx_Oracle as OraCx

def connection():
    """connection on TFR"""
    connect = OraCx.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
    return connect

def check_print():
    query = "Select VIEW_NAME from USER_VIEWS where VIEW_NAME like 'EOC%'"
    output = pd.read_sql(query, connection())
    print(output)


check_print()



