import pandas as pd
import numpy as np
import regex as re
from tqdm import tqdm
import xlrd
from datetime import datetime
import openpyxl
import warnings
import cx_Oracle
warnings.simplefilter('ignore')
import os
import json
import csv

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

query="""select PERM_ID,SEC_PARTY_CMN_NM,SEC_PARTY_ADDR_CITY_NM, SEC_PARTY_ADDR_STATE_CD, FLININGS_COUNT,WK_DUNS, WK_GDUN_NAME, WK_GDUN,FJ_GDUN, FJ_GDUN_NAME, MATCH_IND,FJ_DUN_NAME,FJ_DUNS,DUNS_COUNT, LIST_OF_DUNS from mstrstg.spnv_match_FILE where duns_count>1 and batch_id is null order by duns_count"""
df=pd.read_sql(query, con=conn)
print(df)

for i in tqdm(range(len(df))):
    row = df.loc[i,:]
    print(row)
    a=row['LIST_OF_DUNS']
    print(a)
    if ',,' in a:
        a=a.replace(',,',',')
    print(a)
    x=a.split(',')[1:]
    print(x)
    if row['DUNS_COUNT']>100:
        row['COMMENTS']='more than 1000 duns count, providing only 100'
    else:
        row['COMMENTS']=""
    # Convert all values in the list to integers
    x = list(map(int, x))
    print(x)
    df2=pd.DataFrame()
    for i in x:
        row['FJ_DUNS']=i
        df2=df2.append(row)
    df2=df2[['PERM_ID','SEC_PARTY_CMN_NM','SEC_PARTY_ADDR_CITY_NM', 'SEC_PARTY_ADDR_STATE_CD', 'FLININGS_COUNT','WK_DUNS', 'WK_GDUN_NAME', 'WK_GDUN','FJ_GDUN', 'FJ_GDUN_NAME', 'MATCH_IND','FJ_DUN_NAME','FJ_DUNS','DUNS_COUNT', 'LIST_OF_DUNS','COMMENTS']]
    print(df2)
    df2.fillna('null',inplace=True)
    df2.reset_index(inplace=True, drop=True)
    j=0
    while j<len(df2):  
        valu=tuple(df2.iloc[j])
        valu = tuple([val.replace('"',"'") if isinstance(val, str) else val for val in valu])
        valu = tuple([val.replace("'","''") if isinstance(val, str) else val for val in valu])
        valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])
        valu=str(valu)
        valu=valu.replace("'null'","NULL")
        valu=valu.replace('"',"'")
        valu=valu.replace('.0,',",")
        print(valu)
        insert_query = f"""INSERT INTO MSTRSTG.SPNV_DUNS_DETAILS (PERM_ID,SEC_PARTY_CMN_NM,SEC_PARTY_ADDR_CITY_NM, SEC_PARTY_ADDR_STATE_CD, FLININGS_COUNT,WK_DUNS, WK_GDUN_NAME, WK_GDUN,FJ_GDUN, FJ_GDUN_NAME, MATCH_IND,FJ_DUN_NAME,FJ_DUNS,DUNS_COUNT, LIST_OF_DUNS,COMMENTS)
VALUES {valu}"""

        cursor.execute(insert_query)

        conn.commit()
        j=j+1

cursor.close()
conn.close()

