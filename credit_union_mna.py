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
from openpyxl import load_workbook
import glob
import requests
from bs4 import BeautifulSoup as bs
import csv
import os
import regex as re
import cx_Oracle
import numpy as np
import subprocess

resp = requests.get('https://ncua.gov/analysis/chartering-mergers/merger-activity-insurance-report')
resp.text
soup = bs(resp.text,'html.parser')
list_of_links = []

for i in soup.find_all('a',href = True):
    if 'zip' in str(i):
        list_of_links.append(i)

for i in range(len(list_of_links)):
    url = str(list_of_links[i]).split('"')[1]
    list_of_links[i] = url  

list_of_links=list_of_links[0:2]

for i in range(len(list_of_links)):
    x="https://ncua.gov"+str(list_of_links[i])
    list_of_links[i] = x

os.environ['ORACLE_HOME'] = '/u01/app/oracle/product/19.3.0/client_1'
with open('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Script/stag_credentials.json') as f:
    connection_data = json.load(f)

# Create the connection string
dsn = cx_Oracle.makedsn(connection_data['host'], connection_data['port'], service_name=connection_data['SID'])
conn = cx_Oracle.connect(user=connection_data['user'], password=connection_data['password'], dsn=dsn)
cursor=conn.cursor()

q="""select max(date_) EFFDATE from mstrstg.credit_union_mna
where date_<='01-JUN-22 12.00.00.000000000 AM'"""
da=pd.read_sql(q, con=conn)
print(da)
credit_union_active_query="""select NCUA_ID ID_NCUA,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE, SOURCE FROM mstrstg.CREDIT_UNION_ACTIVE"""
active=pd.read_sql(credit_union_active_query,con=conn)
credit_union_closed_query="""select NCUA_ID ID_NCUA,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE, SOURCE FROM mstrstg.CREDIT_UNION_CLOSED"""
closed=pd.read_sql(credit_union_closed_query,con=conn)


from datetime import datetime, date
i=list_of_links[1]
i=i.split('-activity-')[1]
i=i.split('.zip')[0]
x=i.split('-')[0]
x=x[0:3]
y=i.split('-')[1]
i=x+'-'+y
print(i)
i = datetime.strptime(i, '%b-%Y')
print(type(i))
input_str = da['EFFDATE'][0]
dt_obj = datetime.strptime(str(input_str), '%Y-%m-%d %H:%M:%S')
output_str = dt_obj.strftime('%b-%Y')
output_date = datetime.strptime(output_str, '%b-%Y')
if i<=output_date:
    i=list_of_links[0]
    i=i.split('-activity-')[1]
    i=i.split('.zip')[0]
    x=i.split('-')[0]
    x=x[0:3]
    y=i.split('-')[1]
    i=x+'-'+y
    print(i)
    i = datetime.strptime(i, '%b-%Y')
    input_str = da['EFFDATE'][0]
    dt_obj = datetime.strptime(str(input_str), '%Y-%m-%d %H:%M:%S')
    output_date = datetime.strptime(output_str, '%b-%Y')
    print(i,output_date)
    if i<=output_date:
        exit()
    else:
        list_1=list_of_links[0:1]
else:
    list_1=list_of_links

for i in list_1:
    filename = i.split('activity-')[-1]
    r=requests.get(i)
    open(filename, 'wb').write(r.content)
# convert the input string to datetime object
folder_path = '/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output'
subprocess.run(["ls", "-u", folder_path])
time.sleep(35)

for i in list_1:
    filename = i.split('activity-')[-1]
    open(filename, errors="ignore").read()
    with ZipFile(filename, mode="r") as archive:
        print(filename)
        archive.printdir()
        archive.extractall()
        print('done')
folder_path = '/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Output'
subprocess.run(["ls", "-u", folder_path])
time.sleep(35)

import os
csv_files = glob.glob(os.path.join("insurance-report-activity-detail-*.xlsx"))
list_of_data=[]
for f in csv_files:
    wb=load_workbook(f, read_only=True, keep_links=False)
    df = pd.read_excel(f,sheet_name='Mergers',engine='openpyxl')
    f=f.split('insurance-report-activity-detail-')[1]
    print(list_of_data)
    if ' revised' in f:
        f=f.split(' revised')[0]
    date_string=f.split('.')[0]
    print(date_string)
    dt = datetime.strptime(date_string, '%Y-%m')
    formatted_date = dt.replace(day=1).strftime('%Y-%m-%d %H:%M:%S')
    df['DATE'] = formatted_date
    list_of_data.append(df)
    print(list_of_data)

dfinact=pd.concat(list_of_data,axis=0)
dfinact.reset_index(inplace=True)
dfinact['PARENT_CITY'] = dfinact['Continuing Location'].str.split(',').str[0]
dfinact['PARENT_STATE'] = dfinact['Continuing Location'].str.split(',').str[1]
dfinact['CITY'] = dfinact['Merging Location'].str.split(',').str[0]
dfinact['STATE'] = dfinact['Merging Location'].str.split(',').str[1]
print(dfinact.columns)
dfinact['SOURCE']='CREDIT_UNION_MNA'
def mna_list_naming(df):
    df.rename(columns = {'Merging Credit Union Name':'NAME','Continuing Name':'PARENT_NAME','Continuing Credit Union Charter':'NEW_NCUA_ID','Merging Credit Union Charter':'OLD_NCUA_ID'}, inplace = True)
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
    df['SOURCE']=df['SOURCE']
    df = df.drop_duplicates(subset = ['DATE','OLD_NCUA_ID','ORIGINAL_NAME','ORIGINAL_PARENT_NAME','ORIGINAL_PARENT_CITY','ORIGINAL_CITY','NAME','CITY','STATE','PARENT_NAME','PARENT_CITY','PARENT_STATE','NEW_NCUA_ID'],keep = 'last').reset_index(drop = True)
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
            sen.replace(word,words[word])
    return sen

update_query="""UPDATE mstrstg.credit_union_mna
SET new_event_ind = 0
"""
cursor.execute(update_query)
conn.commit()


query = f"""select*from mstrstg.credit_union_mna"""
df2=pd.read_sql(query, con=conn)



mna=mna_list_naming(dfinact)
mna.rename(columns = {'NAME':'COMPRESSED_NAME','CITY':'COMPRESSED_CITY','PARENT_NAME':'COMPRESSED_PARENT_NAME','PARENT_CITY':'COMPRESSED_PARENT_CITY','OUT_CERT':'FDIC_ID','SUR_CERT':'PARENT_FDIC_ID','DATE':'DATE_'}, inplace = True)
mna.rename(columns = {'ORIGINAL_NAME':'NAME','ORIGINAL_CITY':'CITY','ORIGINAL_PARENT_NAME':'PARENT_NAME','ORIGINAL_PARENT_CITY':'PARENT_CITY'}, inplace = True)
mna['NEW_EVENT_IND']=1
mna=mna.reindex(columns=['OLD_NCUA_ID', 'NEW_NCUA_ID', 'OLD_RSSD_ID','NEW_RSSD_ID','NAME',  'COMPRESSED_NAME', 'CITY','COMPRESSED_CITY','STATE','PARENT_NAME','COMPRESSED_PARENT_NAME','PARENT_CITY','COMPRESSED_PARENT_CITY','PARENT_STATE','SOURCE','DATE_','NEW_EVENT_IND' ])
mna['DATE_'] = pd.to_datetime(mna['DATE_'])
print(mna)
print(df2)
df = pd.DataFrame()
for i in range(len(mna)):
    row = mna.loc[i,:]
    a=row.loc['OLD_NCUA_ID']
    b=row['NEW_NCUA_ID']
    matc=closed[closed.ID_NCUA==a]
    temp=active[active.ID_NCUA==b]
    z=matc.NAME.unique()
    if len(z)>0:
        x=z[0]
        o=temp.NAME.unique()
        if len(o)>0:
            y=o[0]
            row['COMPRESSED_NAME']=x
            row['COMPRESSED_PARENT_NAME']=y
            df = df.append(row)

df=df.append(df2)
print(df)
df['DATE_'] = pd.to_datetime(df['DATE_'])
df.sort_values(by=['DATE_'],inplace=True)
title_change  = df.reset_index()
for i in title_change.index:    
    for j in title_change.index:
        if i < j:
            if title_change['NEW_NCUA_ID'][i]==title_change['OLD_NCUA_ID'][j]:
                title_change['COMPRESSED_PARENT_NAME'][i]=title_change['COMPRESSED_PARENT_NAME'][j]
                title_change['COMPRESSED_PARENT_CITY'][i]=title_change['COMPRESSED_PARENT_CITY'][j]
                title_change['PARENT_STATE'][i]=title_change['PARENT_STATE'][j]
                title_change['NEW_NCUA_ID'][i]=title_change['NEW_NCUA_ID'][j]
                title_change['DATE_'][i]=title_change['DATE_'][j]
                title_change['PARENT_NAME'][i]=title_change['PARENT_NAME'][j]
                title_change['PARENT_CITY'][i]=title_change['PARENT_CITY'][j]
import tqdm


title_change.fillna('null',inplace=True)
print('tc     ',title_change)
title_change.reset_index(inplace=True, drop=True)
df=title_change
print(df)
if len(df)==0:
    exit()
df=df.reindex(columns=['OLD_NCUA_ID', 'NEW_NCUA_ID', 'OLD_RSSD_ID','NEW_RSSD_ID','NAME',  'COMPRESSED_NAME', 'CITY','COMPRESSED_CITY','STATE','PARENT_NAME','COMPRESSED_PARENT_NAME','PARENT_CITY','COMPRESSED_PARENT_CITY','PARENT_STATE','SOURCE','DATE_','NEW_EVENT_IND' ])

delete_query="""TRUNCATE TABLE mstrstg.credit_union_mna"""
cursor.execute(delete_query)
conn.commit()
i=0
while i <len(df):
    valu=tuple(df.iloc[i])
    print(len(valu))
    valu = tuple([val.replace("'","''") if isinstance(val, str) else val for val in valu])
    valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])

    valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])
    print(valu)
    valuss = valu[:15] + (str(valu[15]),) +valu[16:]


    print(valuss)
    a=valuss[15]

    valuw = valuss[:15] + (f"to_timestamp('{a}','YYYY-MM-DD HH24:MI:SS')",)+valuss[16:]

    valuw=str(valuw)
    valus=valuw.replace("'null'","NULL") 
    valus=valus.replace('\'"',"'")
    valus=valus.replace('"\'',"'")
    valus=valus.replace('"',"'")
    valus=valus.replace("'',","',")
    

    valus=valus.replace("'to_timestamp",'TO_TIMESTAMP')
    valus=valus.replace("MM-DD HH24:MI:SS')'","MM-DD HH24:MI:SS')")
    valus=valus.replace('"',"'")
    
    print(valus)


    insert_query = f"""INSERT INTO mstrstg.CREDIT_UNION_MNA (OLD_NCUA_ID, NEW_NCUA_ID, OLD_RSSD_ID,NEW_RSSD_ID,NAME,  COMPRESSED_NAME, CITY,COMPRESSED_CITY,STATE,PARENT_NAME,COMPRESSED_PARENT_NAME,PARENT_CITY,COMPRESSED_PARENT_CITY,PARENT_STATE,SOURCE,DATE_,NEW_EVENT_IND ) VALUES {valus}"""


# execute the INSERT query with the values
    cursor.execute(insert_query)

# commit the transaction
    conn.commit()
    i=i+1

# close the cursor and connection objects
cursor.close()
conn.close()