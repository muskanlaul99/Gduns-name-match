from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import pandas as pd
import selenium
import requests
from zipfile import ZipFile
from selenium.webdriver import ActionChains
import time
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import logging
from itertools import chain
import json
import csv
import os
import regex as re
import cx_Oracle
import numpy as np
import subprocess
os.environ['ORACLE_HOME'] = '/u01/app/oracle/product/19.3.0/client_1'
with open('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Script/stag_credentials.json') as f:
    connection_data = json.load(f)

# Create the connection string
dsn = cx_Oracle.makedsn(connection_data['host'], connection_data['port'], service_name=connection_data['SID'])
conn = cx_Oracle.connect(user=connection_data['user'], password=connection_data['password'], dsn=dsn)
cursor=conn.cursor()



q="""select max(effdate) EFFDATE from mstrstg.fdic_events_file"""
da=pd.read_sql(q, con=conn)
print(da)

#--------------------------------------------START COMMENTING TO STOP DOWNLOADING--------------------------------------------

options = Options()
prefs = {"download.default_directory": "/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/",
         "download.prompt_for_download": False,
         "download.directory_upgrade": True,
         "safebrowsing.enabled": True}
options.add_experimental_option("prefs", prefs)
options.add_argument('--headless')

driver = webdriver.Chrome('/usr/bin/chromedriver', options=options)
time.sleep(10)
from datetime import date, timedelta

today = date.today()-timedelta(days=1)
nd = today.strftime("%d")
nm=today.strftime("%m")
ny=today.strftime("%Y")
from datetime import datetime

# specify the input date string and its format
input_date_string = str(da['EFFDATE'][0])
input_date_format = '%Y-%m-%d %H:%M:%S'

# convert the input string to datetime object
date_object = datetime.strptime(input_date_string, input_date_format)
date_object=date_object + timedelta(days=1)
od = date_object.strftime("%d")
om=date_object.strftime("%m")
oy=date_object.strftime("%Y")

url = f"https://banks.data.fdic.gov/bankfind-suite/oscr?endDate={nm}%2F{nd}%2F{ny}&eventCode=1%20OR%20110%20OR%20221%20OR%20222%20OR%20223%20OR%20225%20OR%20211%20OR%20213%20OR%20215%20OR%20216%20OR%20217%20OR%20820%20OR%20830%20OR%20350%20OR%20360%20OR%20150%20OR%20310%20OR%20320%20OR%20410%20OR%20411%20OR%20420%20OR%20430%20OR%20440%20OR%20470%20OR%20510%20OR%20520%20OR%20230%20OR%20235%20OR%20240%20OR%20260%20OR%20610%20OR%20712%20OR%20520999%20OR%20711%20OR%20721&pageNumber=1&resultLimit=25&searchDateRadio=PROCDATE&sortField=INSTNAME&sortOrder=ASC&startDate={om}%2F{od}%2F{oy}"

list_of_sites=["//*[@id='business-combinations-failures-title-data-cell']/a",'//*[@id="business-combinations-title-data-cell"]/a','//*[@id="liquidations-title-data-cell"]/a',"//*[@id='title-changes-title-data-cell']/a"]

for i in list_of_sites:
    try:
        time.sleep(10)
        driver.get(url)
        time.sleep(10)
        element2=driver.find_element(By.XPATH,i).click()
        time.sleep(10)
        x21=driver.find_element(By.ID,"dataDownload").click()
        time.sleep(10)    
        lr=driver.find_element(By.XPATH,"//*[@id='historyCSVDownload']").click()
        time.sleep(30)
        print('done')
    except:
        print('a')


#---------------------END COMMENTING TO STOP DOWNLOADING---------------------------------------------------


 
folder_path = '/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output'
subprocess.run(["cd", " ", folder_path])
time.sleep(35)

today = date.today()
nd = today.strftime("%d")
nm=today.strftime("%m")
ny=today.strftime("%Y")
if nd[0]=='0':
    nd=nd[1]

if nm[0]=='0':
    nm=nm[1]

print(nm,nd,ny)

import glob
list_1=[]
path_list = [f'/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/Business_Combinations_-_Failures_{nm}_{nd}_{ny}.csv',f'/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/Business_Combinations_{nm}_{nd}_{ny}.csv',f'/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/Title_Changes_{nm}_{nd}_{ny}.csv',f'/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/Liquidations_{nm}_{nd}_{ny}.csv']
path_list_2=['MNA','MNA','TC','LQ']
for i in range(len(path_list)):
    try:
        df2 = pd.read_csv(path_list[i])
        df2['SOURCE']=path_list_2[i]
        list_1.append(df2)
        print(i)
    except:
        print('No matching file found.')
print(list_1)

if len(list_1)==0:
    exit()

def preprocess_text(sentence):
    try:
        sentence=re.sub("\(.*?\)", "", sentence)
    except:
        print('k')
    sentence=str(sentence).lower()
    sentence=sentence.replace(" ","")

    #Remove punctuations
    sentence = re.sub('[^a-zA-Z0-9]', ' ', sentence)
    # Special character removal
    sentence = re.sub(r"\s+[a-zA-Z]\s+", ' ', sentence)
    # Removing multiple spaces
    sentence = re.sub(r'\s+', ' ', sentence)
    #remove numbers
    sentence = re.sub(r'[0-9]',' ',sentence)
    #split multiple lines
    sentence = re.split("/",sentence)[-1]

    return sentence.strip().replace(" ","")

def filter_word(sen):
    try:
        sen=re.sub("\(.*?\)", "", sen)
    except:
        print('k')
    sen=str(sen).lower()
    if ',' in sen:
        sen=sen.split(',')[0]
    if ' as ' in sen:
        sen=sen.split(' as')[0]
    
    words = [' administrativeagent',
            ' administrative agent',
            ' collateral agent',
            ' collateral agent',
            ' facility agent',
            ' facility agent',
            ' agent',
            ' structuring',
            ' secured creditor',
            ' securedcreditor',
            ' assignee',
            ' SECURED PARTY',
            ' SECUREDPARTY',
            ' Individual capacity',
            ' Individualcapacity',
            ' as ownwer',
            ' asownwer',
            ' as trustee',
            ' astrustee',
            ' Trust admin',
            ' Trustadmin',
            ' formerly known',
            ' also known' ,
            ' as known as',
            ' formerly known',
            ' also known as',
            ' Trust administration',
            ' Trustadministration',
            ' owner trustee',
            ' ownertrustee',
            ' as successor by assignment',
            ' assuccessorbyassignment',
            ' holders of',
            ' holdersof',
            ' through certificates',
            ' throughcertificates',
            ' as trustee or the benefit of the holder',
            ' astrusteeorthebenefitoftheholder',
            ' as trustee or registered holder',
            ' on behalf of',
            ' onbehalfof',        
            ' capacity as',
            ' capacityas',
            ' successor or in interest',
            ' successororininterest',
            ' by merger to',
            ' bymergerto',
            ' solely as nominee',
            ' solelyasnominee',
            ' isaoa',
            ' ISAOAATI',
            ' ISAOAATIMA',
            ' FKA',
            ' for istelf & agent',
            ' foristelf&agent',
            ' a division of',
            ' adivisionof',
            ' attorney general',
            ' attorneygeneral',
            ' solely',
            ' not individually',
            ' notindividually',
            ' but solely',
            ' butsolely',
            ' collateral',
            ' trustee',
            ' certificate',
            ' successor',
            ' formerly',
            ' as purchase',
            ' aspurchase',
            ' notinits',
            ' not inits',
            ' as master',
            ' asmaster',
            ' as indenture',
            ' asindenture',
            ' asmortage',
            ' as mortage',
            ' asoffshore',
            ' as off shore',
            ' as offshore',
            ' asprogram',
            ' as program',
            ' astax',
            ' as tax',
            ' asfinal',
            ' as final',
            ' asfiscal',
            ' as fiscal',
            ' asdeposite',
            ' as deposite',
            ' ascustodian',
            ' as custodian',
            ' asbond',
            ' as bond',
            ' assecurity',
            ' as security',
            ' llc',
            ' d/b/a',
            ' loan operations',
            ' ATTN',
            ' attention',
            ' dba',
            ' limited']
    
    for word in words:
        if word.lower() in sen:
            n = sen.find(word.lower())
            sen = sen[:n]
            
    if "as" == sen[-2:]:
        sen == sen[:-2]
    if "na" == sen[-2:]:
        sen = sen[:-2]
    if "inc" == sen[-3:]:
        sen = sen[:-3]      
    if '&' in sen:
        sen=sen.replace('&', 'and')
    return sen

def city_change(sen):
    words = {'saint':'st',
            'nyc':'newyork',
            'ny':'newyork',
            'mount':'mt'}
    for word in words:
        if word in sen:
            sen=sen.replace(word,words[word])
    return sen

def inactive_list_naming(df):
    if 'FRM_INSTNAME' in df.columns:
        df.rename(columns = {'FRM_INSTNAME':'NAME','FRM_PCITY':'CITY','FRM_PSTALP':'OLD_STATE','INSTNAME':'PARENT_NAME','PCITY':'PARENT_CITY','PSTALP':'PARENT_STATE'}, inplace = True)
        df['ORIGINAL_NAME']=df['NAME']
        df['ORIGINAL_CITY']=df['CITY']
        df['ORIGINAL_PARENT_NAME']=df['PARENT_NAME']
        df['ORIGINAL_PARENT_CITY']=df['PARENT_CITY']
        df['NAME']=df['NAME'].apply(filter_word)
        df['NAME']=df['NAME'].apply(preprocess_text)     
        df['CITY']=df['CITY'].apply(preprocess_text)
        df['CITY']=df['CITY'].apply(city_change)
        df['PARENT_NAME'] = df['PARENT_NAME'].apply(filter_word)
        df['PARENT_NAME']=df['PARENT_NAME'].apply(preprocess_text)
        
        df['PARENT_CITY']=df['PARENT_CITY'].apply(preprocess_text)
        df['PARENT_CITY']=df['PARENT_CITY'].apply(city_change)
        df['SUR_CERT']=df['CERT']
        df['OUT_CERT']=df['CERT']

    else:
        df.rename(columns = {'OUT_INSTNAME':'NAME','OUT_PCITY':'CITY','OUT_PSTALP':'OLD_STATE','SUR_INSTNAME':'PARENT_NAME','SUR_PCITY':'PARENT_CITY','SUR_PSTALP':'PARENT_STATE'}, inplace = True)
        df['ORIGINAL_NAME']=df['NAME']
        df['ORIGINAL_CITY']=df['CITY']
        df['ORIGINAL_PARENT_NAME']=df['PARENT_NAME']
        df['ORIGINAL_PARENT_CITY']=df['PARENT_CITY']        
        df['NAME'] = df['NAME'].apply(filter_word)
        df['NAME']=df['NAME'].apply(preprocess_text)        
        df['CITY']=df['CITY'].apply(preprocess_text)
        df['CITY']=df['CITY'].apply(city_change)
        df['PARENT_NAME'] = df['PARENT_NAME'].apply(filter_word)
        df['PARENT_NAME']=df['PARENT_NAME'].apply(preprocess_text)        
        df['PARENT_CITY']=df['PARENT_CITY'].apply(preprocess_text)
        df['PARENT_CITY']=df['PARENT_CITY'].apply(city_change)
        df['SUR_CERT']=df['SUR_CERT']
        df['OUT_CERT']=df['OUT_CERT']
        
    if 'EFFDATE' in df.columns:
        df['DATE'] = df['EFFDATE']
    if 'SOURCE' in df.columns:
        df['SOURCE']=df['SOURCE']
    else:
        df['SOURCE']=" "
    df=df.reset_index()

    return df


def data_prep_inactive(dflist):
    df_clnd=[]
    for i in dflist:

        i=inactive_list_naming(i)

        temp=i[['SUR_CERT','OUT_CERT','NAME','ORIGINAL_NAME','CITY','ORIGINAL_CITY','SOURCE','PARENT_NAME','OLD_STATE','PARENT_STATE','ORIGINAL_PARENT_NAME','PARENT_CITY','ORIGINAL_PARENT_CITY','DATE']]

        df_clnd.append(temp)
    dfinact=pd.concat(df_clnd,ignore_index=True)
    #dfinact.insert(2,'fdic_inactive',dfinact['OLD_NAME']+"#"+dfinact['OLD_CITY']+"#"+df_inact['OLD_STATE'])
    return dfinact
inact = data_prep_inactive(list_1)

inact = inact[inact.DATE != 'Savings']
inact = inact[inact.DATE != 'N/A']

inact.sort_values(by=['DATE'],inplace=True)
title_change  = inact.reset_index()
print(title_change)

title_change.dropna(inplace=True)
title_change.reset_index(inplace=True, drop=True)

update_query="""UPDATE mstrstg.fdic_events_file
SET new_event_ind = 0
"""
cursor.execute(update_query)
conn.commit()


query = f"""select*from mstrstg.FDIC_EVENTS_FILE"""
df2=pd.read_sql(query, con=conn)



title_change.rename(columns = {'NAME':'COMPRESSED_NAME','CITY':'COMPRESSED_CITY','PARENT_NAME':'COMPRESSES_PARENT_NAME','OLD_STATE':'STATE','PARENT_CITY':'COMPRESSES_PARENT_CITY','OUT_CERT':'FDIC_ID','SUR_CERT':'PARENT_FDIC_ID','DATE':'EFFDATE'}, inplace = True)
title_change.rename(columns = {'ORIGINAL_NAME':'NAME','ORIGINAL_CITY':'CITY','ORIGINAL_PARENT_NAME':'PARENT_NAME','ORIGINAL_PARENT_CITY':'PARENT_CITY'}, inplace = True)
title_change['NEW_EVENT_IND']=1
df=title_change.reindex(columns=['FDIC_ID', 'NAME', 'COMPRESSED_NAME', 'CITY','COMPRESSED_CITY', 'STATE','PARENT_FDIC_ID', 'PARENT_NAME', 'COMPRESSES_PARENT_NAME', 'PARENT_CITY','COMPRESSES_PARENT_CITY', 'PARENT_STATE','EFFDATE','SOURCE','NEW_EVENT_IND'])
print(df)
print(df2)
df=df.append(df2)
print(df)
df.reset_index(inplace=True, drop=True)
for i in df.index:    
    for j in df.index:
        if i < j:
            if df['PARENT_FDIC_ID'][i]==df['FDIC_ID'][j]:
                df['COMPRESSES_PARENT_NAME'][i]=df['COMPRESSES_PARENT_NAME'][j]
                df['COMPRESSES_PARENT_CITY'][i]=df['COMPRESSES_PARENT_CITY'][j]
                df['PARENT_STATE'][i]=df['PARENT_STATE'][j]
                df['PARENT_FDIC_ID'][i]=df['PARENT_FDIC_ID'][j]
                df['EFFDATE'][i]=df['EFFDATE'][j]
                df['PARENT_NAME'][i]=df['PARENT_NAME'][j]
                df['PARENT_CITY'][i]=df['PARENT_CITY'][j]
                a=df['SOURCE'][i]
                b=df['SOURCE'][j]
                df['SOURCE'][i]=a+'+'+b

e_query="""DELETE mstrstg.fdic_events_file"""
cursor.execute(e_query)
conn.commit()

df.fillna('null',inplace=True)
df.reset_index(inplace=True, drop=True)
print(df)
print(len(df))
i=0
while i <len(df):
    valu=tuple(df.iloc[i])

    valu = tuple([val.replace("'","''") if isinstance(val, str) else val for val in valu])
    valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])

    valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])

    valuw = valu[:12] + (str(valu[12]),) + valu[13:]
    valuss = valuw[:12] + (valuw[12].replace("T",' '),) + valuw[13:]

    a=valuss[12]
    valus = valuss[:12] + (f"to_timestamp('{a}','YYYY-MM-DD HH24:MI:SS')",) + valuss[13:]    
    valus=str(valus)
    valus=valus.replace("'null'","NULL")

    valus=valus.replace('"',"'")
    

    valus=valus.replace("'to_timestamp",'TO_TIMESTAMP')
    valus=valus.replace("MM-DD HH24:MI:SS')'","MM-DD HH24:MI:SS')")
    valus=valus.replace('"',"'")
    valus=valus.replace("')'",'")"')
    print(valus)


    insert_query = f"""INSERT INTO mstrstg.FDIC_events_FILE (FDIC_ID, NAME, COMPRESSED_NAME, CITY,COMPRESSED_CITY, STATE,PARENT_FDIC_ID, PARENT_NAME, COMPRESSES_PARENT_NAME, PARENT_CITY,COMPRESSES_parent_CITY, PARENT_STATE,EFFDATE,SOURCE,NEW_EVENT_IND) VALUES {valus}"""


# execute the INSERT query with the values
    cursor.execute(insert_query)

# commit the transaction
    conn.commit()
    i=i+1

# close the cursor and connection objects
cursor.close()
conn.close()