import pandas as pd
import numpy as np
import regex as re
from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm
import xlrd
from datetime import datetime
import openpyxl
import warnings
import cx_Oracle
from joblib import Parallel, delayed
import logging
warnings.simplefilter('ignore')
import os
import time
import json

#-------------------opening JSON file as connection data---------------------------------
with open('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Script/stag_credentials.json') as f:
    connection_data = json.load(f)

#---------------reading oracle client library path and ld library path------------------
os.environ['ORACLE_HOME'] = connection_data['oracle_client']
os.environ['LD_LIBRARY_PATH'] = connection_data['ld_library']


# Create the connection string
dsn = cx_Oracle.makedsn(connection_data['host'], connection_data['port'], service_name=connection_data['SID'])
conn = cx_Oracle.connect(user=connection_data['user'], password=connection_data['password'], dsn=dsn)
cursor=conn.cursor()

q="""  SELECT distinct PERM_ID,SEC_PARTY_CMN_NM,SEC_PARTY_COMPRESS_CMN_NM, SEC_PARTY_ADDR_CITY_NM,SEC_PARTY_ADDR_STATE_CD,FLININGS_COUNT,WK_DUNS,WK_GDUN_NAME,WK_GDUN,COMPRESSES_SEC_PARTY_NAME,COMPRESSES_SEC_PARTY_CITY,COMPRESSES_SEC_PARTY_STATE,FJ_GDUN,FJ_GDUN_NAME,MATCH_IND,FJ_DUN_NAME,FJ_DUNS,DUNS_COUNT, FINAL_FDIC_PARENT FDIC_PARENT,SOURCE, DETAIL_INFO, COMMENTS, CREDIT_UNION_PARENT, LIST_OF_DUNS, null BATCH_ID FROM mstrstg.spnv_match_raw
where temp2 is null"""
wru=pd.read_sql(q, con=conn)
print(wru)
time.sleep(30)
wru.fillna('null',inplace=True)
wru.reset_index(inplace=True, drop=True)
i=0
while i< len(wru):
    valu=tuple(wru.iloc[i])
    valu = tuple([val.replace('"',"'") if isinstance(val, str) else val for val in valu])
    valu = tuple([val.replace("'","''") if isinstance(val, str) else val for val in valu])
    valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])
    valu=str(valu)
    valu=valu.replace("'null'","NULL")
    valu=valu.replace('"',"'")

    insert_query = f"""INSERT INTO mstrstg.spnv_match_FILE (PERM_ID,SEC_PARTY_CMN_NM,SEC_PARTY_COMPRESS_CMN_NM,SEC_PARTY_ADDR_CITY_NM,SEC_PARTY_ADDR_STATE_CD,
FLININGS_COUNT,WK_DUNS,WK_GDUN_NAME , WK_GDUN,COMPRESSES_SEC_PARTY_NAME,COMPRESSES_SEC_PARTY_CITY,
COMPRESSES_SEC_PARTY_STATE, 
FJ_GDUN,      
FJ_GDUN_NAME,
MATCH_IND,  
FJ_DUN_NAME,  
FJ_DUNS,   
DUNS_COUNT, 
FDIC_PARENT,     
SOURCE,  
DETAIL_INFO,          
COMMENTS,            
CREDIT_UNION_PARENT,     
LIST_OF_DUNS,BATCH_ID) VALUES {valu}"""
    cursor.execute(insert_query)

    conn.commit()
    i=i+1
update_query="""UPDATE mstrstg.spnv_match_raw
SET TEMP2 = 'N'
WHERE TEMP2 is NULL"""

cursor.execute(update_query)
conn.commit()
i=i+1

cursor.close()
conn.close()