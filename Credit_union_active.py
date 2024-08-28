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
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)
from itertools import chain
import json
import csv
import os
import subprocess
import regex as re
import cx_Oracle
url = "https://www.ffiec.gov/npw/FinancialReport/DataDownload"
import numpy as np

#---------------------START COMMENTING TO SKIP THE DOWNLOADING-----------------------------------------------------------------------------

options = Options()
prefs = {"download.default_directory": "/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/",
         "download.prompt_for_download": False,
         "download.directory_upgrade": True,
         "safebrowsing.enabled": True}
options.add_experimental_option("prefs", prefs)
options.add_argument('--headless')

driver = webdriver.Chrome('/usr/bin/chromedriver', options=options)
time.sleep(10)
time.sleep(4)
time.sleep(5)
driver.get(url)
time.sleep(5)
element2=driver.find_element(By.XPATH,"/html/body/div[4]/div[2]/div[4]/div[2]/div/ul/li[1]/button")
time.sleep(10)
element2.click()
time.sleep(140)


#--------------------END COMMENTING FOR STOPING THE DOWNLOAD--------------------------------------------------





folder_path = '/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output'

subprocess.run(["ls", "-u", folder_path])
time.sleep(15)


from zipfile import ZipFile
with ZipFile('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/CSV_ATTRIBUTES_ACTIVE.zip', mode="r") as archive:
    archive.printdir()
    archive.extractall()
folder_path = '/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output'

subprocess.run(["ls", "-u", folder_path])
time.sleep(25)

df = pd.read_csv('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output/CSV_ATTRIBUTES_ACTIVE.CSV')
df['SOURCE']='CREDIT_UNION_ACTIVE'
df=df[df.ID_NCUA!=0]

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


def active_list_naming(df):
    df.rename(columns = {'NM_LGL':'NAME','CITY':'CITY','STATE_ABBR_NM':'STATE'}, inplace = True)
    df['ORIGINAL_NAME']=df['NAME']
    df['NAME'] = df['NAME'].apply(filter_word)
    df['NAME']=df['NAME'].apply(preprocess_text)
    df['ORIGINAL_CITY']=df['CITY']
    df['CITY']=df['CITY'].apply(preprocess_text)
    df['CITY']=df['CITY'].apply(city_change)
    df['SOURCE']=df['SOURCE']
    df = df.drop_duplicates(subset = ['ID_NCUA','NAME','CITY','STATE','ORIGINAL_NAME','ORIGINAL_CITY'],keep = 'last').reset_index(drop = True)
    return df

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
            ' loan operations',
            ' D/B/A',    
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

query = f"""TRUNCATE TABLE mstrstg.CREDIT_UNION_ACTIVE"""
cursor.execute(query)
conn.commit()

df=active_list_naming(df)
df.rename(columns = {'NAME':'COMPRESSED_NAME','CITY':'COMPRESSED_CITY','ID_NCUA':'NCUA_ID'}, inplace = True)
df.rename(columns = {'ORIGINAL_NAME':'NAME','ORIGINAL_CITY':'CITY'}, inplace = True)

temp=df.reindex(columns=['ID_RSSD','NCUA_ID','NAME','COMPRESSED_NAME','CITY','COMPRESSED_CITY','STATE','SOURCE'])

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


    insert_query = f"""INSERT INTO mstrstg.CREDIT_UNION_ACTIVE (ID_RSSD,NCUA_ID,NAME,COMPRESSED_NAME,CITY,COMPRESSED_CITY,STATE,SOURCE) VALUES {valu}"""


# execute the INSERT query with the values
    cursor.execute(insert_query)

# commit the transaction
    conn.commit()
    i=i+1

# close the cursor and connection objects
cursor.close()
conn.close()
