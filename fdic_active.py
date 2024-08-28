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

# --------------------------START COMMENTING TO STOP THE DOWNLOAD-----------------------------------------------------
options = Options()
prefs = {"download.default_directory": "/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/",
         "download.prompt_for_download": False,
         "download.directory_upgrade": True,
         "safebrowsing.enabled": True}
options.add_experimental_option("prefs", prefs)
options.add_argument('--headless')

driver = webdriver.Chrome('/usr/bin/chromedriver', options=options)
time.sleep(10)
url = "https://banks.data.fdic.gov/docs/"
time.sleep(2)
driver.get(url)
time.sleep(5)
element2=driver.find_element(By.XPATH,'//*[@id="main-content"]/ul[1]/li[4]/a')
time.sleep(5)
element2.click()
print('done')
time.sleep(150)
driver.quit()
folder_path = '/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output'
subprocess.run(["ls", "-u", folder_path])
time.sleep(15)



#----------------------------END COMMENTING TO STOP DOWNLOADING-------------------------------------------




df=pd.read_csv('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/locations.csv')
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
            ' d/b/a',
            ' loan operations',
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
            ' ATTN',
            ' attention',
            ' dba',
            ' limited']
    
    for word in words:
        if word.lower() in sen:
            n = sen.find(word.lower())
            sen = sen[:n]
            
    if " as" == sen[-3:]:
        sen == sen[:-3]
    if " na" == sen[-3:]:
        sen = sen[:-3]
    if " inc" == sen[-4:]:
        sen = sen[:-4]      
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

def active_list_naming(df):
    df.rename(columns = {'NAME':'NAME','CITY':'CITY','STALP':'STATE'}, inplace = True)
    df['ORIGINAL_NAME']=df['NAME']
    df['NAME'] = df['NAME'].apply(filter_word)
    df['NAME']=df['NAME'].apply(preprocess_text)
    df['ORIGINAL_CITY']=df['CITY']
    df['CITY']=df['CITY'].apply(preprocess_text)
    df['CITY']=df['CITY'].apply(city_change)
    df['SOURCE']='FDIC_ACTIVE'
    df = df.drop_duplicates(subset = ['CERT','NAME','CITY','STATE','MAINOFF'],keep = 'last').reset_index(drop = True)
    return df
query = f"""TRUNCATE TABLE mstrstg.FDIC_ACTIVE_FILE"""
cursor.execute(query)
conn.commit()

df=active_list_naming(df)
df.rename(columns = {'NAME':'COMPRESSED_NAME','CITY':'COMPRESSED_CITY','CERT':'FDIC_ID'}, inplace = True)
df.rename(columns = {'ORIGINAL_NAME':'NAME','ORIGINAL_CITY':'CITY'}, inplace = True)

temp=df.reindex(columns=['FDIC_ID','NAME','COMPRESSED_NAME','CITY','COMPRESSED_CITY','STATE','MAINOFF','SOURCE'])

temp.fillna('null',inplace=True)
temp.reset_index(inplace=True, drop=True)
i=0
while i <len(temp):
    valu=tuple(temp.iloc[i])
    print(len(valu))
    valu = tuple([val.replace("'","''") if isinstance(val, str) else val for val in valu])
    valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])

    valu=str(valu)
    valu=valu.replace("'null'","NULL")

    valu=valu.replace('"',"'")
    print(valu)


    insert_query = f"""INSERT INTO mstrstg.FDIC_ACTIVE_FILE (FDIC_ID, NAME, COMPRESSED_NAME, CITY,COMPRESSED_CITY, STATE,MAINOFF,SOURCE) VALUES {valu}"""


# execute the INSERT query with the values
    cursor.execute(insert_query)

# commit the transaction
    conn.commit()
    i=i+1

# close the cursor and connection objects
cursor.close()
conn.close()