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
import json
import time

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

query="""select PERM_ID,SEC_PARTY_CMN_NM,SEC_PARTY_COMPRESS_CMN_NM,SEC_PARTY_ADDR_CITY_NM,SEC_PARTY_ADDR_STATE_CD,
FLININGS_COUNT,WK_DUNS,WK_GDUN_NAME , WK_GDUN,COMPRESSES_SEC_PARTY_NAME,COMPRESSES_SEC_PARTY_CITY,
COMPRESSES_SEC_PARTY_STATE, 
FJ_GDUN,      
FJ_GDUN_NAME,
MATCH_IND,  
FJ_DUN_NAME,  
FJ_DUNS,   
DUNS_COUNT, 
FINAL_FDIC_PARENT FDIC_PARENT,     
SOURCE,  
DETAIL_INFO,          
COMMENTS,            
CREDIT_UNION_PARENT,     
LIST_OF_DUNS,
TEMP1
FROM mstrstg.spnv_match_raw
WHERE TEMP2 ='Y'"""
wru=pd.read_sql(query, con=conn)
print(wru)
time.sleep(30)
wru.fillna('null',inplace=True)
rust=wru.TEMP1.unique()
for i in rust:
    matc=wru[wru.TEMP1==i]
    matc.reset_index(inplace = True,drop = True)
    a=matc['PERM_ID'][0]
    b=str(matc['SEC_PARTY_CMN_NM'][0])
    c=str(matc['SEC_PARTY_COMPRESS_CMN_NM'][0])
    d=str(matc['SEC_PARTY_ADDR_CITY_NM'][0])
    e=str(matc['SEC_PARTY_ADDR_STATE_CD'][0])
    f=matc['FLININGS_COUNT'][0]
    g=matc['WK_DUNS'][0]
    h=str(matc['WK_GDUN_NAME'][0])
    i=matc['WK_GDUN'][0]
    j=str(matc['COMPRESSES_SEC_PARTY_NAME'][0])
    k=str(matc['COMPRESSES_SEC_PARTY_CITY'][0])
    l=str(matc['COMPRESSES_SEC_PARTY_STATE'][0])
    m=-45000
    ru=0
    xe='multiple fdic parents gduns: '
    while ru<len(matc):
        xe=xe+','+str(matc['FJ_GDUN_NAME'][ru])
        ru=ru+1
    n=xe
    o='null' 
    rus=0
    xey='multiple fdic parents duns: '
    while rus<len(matc):
        xey=xey+','+str(matc['FJ_DUN_NAME'][rus])
        rus=rus+1
    p=xey
    q=-1
    rust=0
    xeye=0
    while rust<len(matc):
        xeye=xeye+int(matc['DUNS_COUNT'][rust])
        rust=rust+1
    r= xeye
    rusty=0
    xeyey=''
    while rusty<len(matc):
        xeyey=xeyey+','+str(matc['FDIC_PARENT'][rusty])
        rusty=rusty+1
    s=xeyey
    rm=0
    xm=''
    while rm<len(matc):
        xm=xm+','+str(matc['SOURCE'][rm])
        rm=rm+1
    t=xm
    u=str(matc['DETAIL_INFO'][0])
    rn=0
    xn='multiple fdic parents gduns: '
    while rn<len(matc):
        xn=xn+','+str(matc['FJ_GDUN'][rn])
        rn=rn+1              
    v=str(xn)
    w=str(matc['CREDIT_UNION_PARENT'][0])
    xeke=''
    rusk=0
    while rusk<len(matc):
        xeke=xeke+str(matc['LIST_OF_DUNS'][rusk])
        rusk=rusk+1
    x=xeke
    if r>100:
        result = ','.join(s.split(',')[:101])
        print(r)
        print(x)
    y='null'
    tup=(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y)
    tup=str(tup)
    print(tup)
    tup=tup.replace("'null'","NULL")

    tup=tup.replace('"',"'")
    time.sleep(20)
  
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
LIST_OF_DUNS, BATCH_ID) VALUES {tup}"""
    try:
    	cursor.execute(insert_query)

    	conn.commit()
    except:
        print('')

update_query="""UPDATE mstrstg.spnv_match_raw
SET TEMP2 = 'N'
WHERE TEMP2='Y'"""
cursor.execute(update_query)
conn.commit()

cursor.close()
conn.close()

