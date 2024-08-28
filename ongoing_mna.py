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
from itertools import chain
import json
import csv


os.environ['ORACLE_HOME'] = '/u01/app/oracle/product/19.3.0/client_1'

with open('stag_credentials.json') as f:
    connection_data = json.load(f)

# Create the connection string
dsn = cx_Oracle.makedsn(connection_data['host'], connection_data['port'], service_name=connection_data['SID'])
conn = cx_Oracle.connect(user=connection_data['user'], password=connection_data['password'], dsn=dsn)
cursor=conn.cursor()
sq4="""select distinct  duns_num, company, city, st, gl_ult_dun, gl_ult_nam from core.wk_fullfile
where cur_rec_ind='Y' and gl_ult_dun in (select gl_ult_dun from core.wk_fullfile where ln_of_bus in ('Accident and health insurance',
'Accident/health insurance carrier',
'Bank holding companies',
'Bank holding company',
'Commercial bank',
'Commercial banks, not chartered',
'Credit reporting services',
'Depository banking services',
'Federal & federally sponsored credit,nsk',
'Federal credit agency',
'Federal credit union',
'Federal credit unions',
'Federal reserve bank',
'Federal reserve banks, nsk',
'Federal savings institution',
'Federal savings institutions',
'Financial services',
'Fire, marine, and casualty insurance',
'Fire/casualty insurance carrier',
'Foreign bank and branches and agencies',
'Foreign bank/branch/agent',
'Foreign trade and international banks,nsk',
'Foreign trade/international bank',
'Functions related to deposit banking, nsk',
'Holding companies, nec, nsk',
'Holding company',
'Insurance agent/broker',
'Insurance agents nec',
'Insurance agents, brokers, and service, n',
'Insurance agents,brokers,and service,nsk',
'Insurance carrier',
'Insurance carriers, nec, nsk',
'Investment holding companies except banks',
'Investment offices, nec',
'Life insurance carrier',
'Life insurance carriers',
'Life insurance, nsk',
'Misc business credit institutions',
'Misc. business credit institutions,nsk',
'Mortgage banker/correspondent',
'Mortgage bankers and correspondents, nsk',
'National commercial bank',
'National commercial banks, nsk',
'Nondeposit trust facilities',
'Nondeposit trust facility',
'Personal credit institution',
'Personal credit institutions',
'Personal holding companies except banks',
'Real estate title insurance',
'Savings institutions, except federal, nsk',
'Short-term business credit institution',
'Short-term business credit, nsk',
'State commercial bank',
'State commercial banks',
'State credit union',
'State credit unions, nsk',
'Surety insurance carrier',
'Surety insurance, nsk',
'Title insurance',
'Title insurance carrier',
'Business consulting, nec, nsk',
'Business services, nec, nsk',
'Investment advice',
'Investors, nec',
'Loan broker',
'Loan brokers',
'Motorcycle dealers, nsk',
'Real estate agents and managers',
'Real estate investment trust',
'Real property lessors, nec, nsk',
'Savings institution',
'Security broker/dealer',
'Security brokers and dealers, nsk',
'Truck rental/leasing',
'Trust management'
)
and cur_rec_ind='Y')"""
dnb=pd.read_sql(sq4, con=conn)
print (dnb)

fdic_query="""select fdic_id CERT,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,MAINOFF,STATE, SOURCE FROM mstrstg.FDIC_active_FILE"""
fdic=pd.read_sql(fdic_query,con=conn)

fdic_events_query="""select fdic_id OLD_ID,NAME ORIGINAL_NAME, CITY ORIGINAL_CITY,PARENT_NAME ORIGINAL_PARENT_NAME, PARENT_CITY ORIGINAL_PARENT_CITY,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE, PARENT_FDIC_ID NEW_ID, COMPRESSES_PARENT_NAME PARENT_NAME, COMPRESSES_PARENT_CITY PARENT_CITY, PARENT_STATE, EFFDATE, SOURCE FROM mstrstg.FDIC_events_FILE where NEW_EVENT_IND in (1)"""
title_change=pd.read_sql(fdic_events_query,con=conn)
print(title_change)
credit_union_active_query="""select NCUA_ID ID_NCUA,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE, SOURCE FROM mstrstg.CREDIT_UNION_ACTIVE"""
active=pd.read_sql(credit_union_active_query,con=conn)
credit_union_closed_query="""select NCUA_ID ID_NCUA,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE, SOURCE FROM mstrstg.CREDIT_UNION_CLOSED"""
closed=pd.read_sql(credit_union_closed_query,con=conn)
credit_union_mna_query="""SELECT OLD_NCUA_ID OLD_ID, NEW_NCUA_ID NEW_ID,NAME ORIGINAL_NAME, CITY ORIGINAL_CITY,PARENT_NAME ORIGINAL_PARENT_NAME, PARENT_CITY ORIGINAL_PARENT_CITY, COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE,COMPRESSED_PARENT_NAME PARENT_NAME,COMPRESSED_PARENT_CITY PARENT_CITY,PARENT_STATE,SOURCE,DATE_ FROM mstrstg.CREDIT_UNION_MNA WHERE NEW_EVENT_IND in (1)"""
mna=pd.read_sql(credit_union_mna_query,con=conn)
print(mna)
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
            ' d/b/a',
            ' loan operations',
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
            sen=sen.replace(word,words[word])
    return sen

def dnb_list_naming(dnb):
    dnb.rename(columns = {'COMPANY':'NAME','CITY':'CITY','ST':'STATE'}, inplace = True)
    dnb['NAME'] = dnb['NAME'].apply(filter_word)
    dnb['NAME']=dnb['NAME'].apply(preprocess_text)    
    dnb['CITY']=dnb['CITY'].apply(preprocess_text)
    dnb['CITY']=dnb['CITY'].apply(city_change)
    return dnb

dnb = dnb_list_naming(dnb)

#ngram function
def ngrams(string, ns=[1,2,3], weights={'creditunion':1,'federalcreditunion':1,'bank':1}):
    string = re.sub(r'[,-./]|\sBD',r'', string)
    result = []
    for n in ns:
        ngrams = zip(*[string[i:] for i in range(n)])
        for ngram in ngrams:
            ngram_str = ''.join(ngram)
            if ngram_str in weights:
                result.append((ngram_str, weights[ngram_str]))
            else:
                result.append((ngram_str, 1))
    return result

#Nearest neighbour function
def create_KNN(active_lookup):
    vectorizer = TfidfVectorizer(min_df=1, tokenizer=lambda x: [ngram[0] for ngram in ngrams(x)],
                                 lowercase=False, norm=None)
    tf_idf_active_lookup = vectorizer.fit_transform(active_lookup)
    weights={'creditunion':1,'federalcreditunion':1,'bank':1}
    for i, ngram in enumerate(vectorizer.get_feature_names()):

        if ngram in weights:
            tf_idf_active_lookup[:, i] *= weights[ngram]
    nbrs_act = NearestNeighbors(n_neighbors=1, n_jobs=-1, metric="cosine").fit(tf_idf_active_lookup)
    return vectorizer, nbrs_act

mna['REASON']='NIC'
title_change['REASON']='FDIC'
fdic_vector,fdic_knn = create_KNN(fdic['NAME'])
if len(title_change)>0:
    title_change_vector,title_change_knn = create_KNN(title_change['NAME'])
active_vector,active_knn = create_KNN(active['NAME'])
closed_vector,closed_knn = create_KNN(closed['NAME'])
if len(mna)>0:
    mna_vector,mna_knn = create_KNN(mna['NAME'])
dnb.reset_index(inplace = True,drop = True)
dnb_vector,dnb_knn = create_KNN(dnb['NAME'])
fdic['STATE']=fdic['STATE'].replace(np.nan, " ")
active['STATE']=active['STATE'].replace(np.nan, " ")
if len(mna)>0:
    mna.reset_index(inplace=True, drop=True)
if len(title_change)>0:
    title_change.reset_index(inplace=True, drop=True)

def dnb_proccess_old(row,col):
    name = row[col]
    city = row['CITY']    
    vec = dnb_vector.transform([name])
    dist,ind = dnb_knn.kneighbors(vec)
    dnb_name=dnb.loc[ind[0][0],'NAME']
    #checking the closest match distance is is less that the threshold 0.2
    if dist[0][0]<=0.2:
        #creating a datframe of dnb for the closest match duns name
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        matc_vector,matc_knn = create_KNN(matc['CITY'])
        vec = matc_vector.transform([city])
        city_dist,city_ind = matc_knn.kneighbors(vec)
        dnb_city=matc.loc[city_ind[0][0],'CITY']
	#checking for the matc dataframe if teh city match is less than 0.2
        if city_dist[0][0]<=0.2:
            matc1=matc[matc.CITY==dnb_city]
            matc1.reset_index(inplace=True, drop=True)
            #check if the gdun is unique or not
            if len(matc1.GL_ULT_DUN.unique())==1:
                row['gdun_name_old'] = matc.loc[city_ind[0][0],'GL_ULT_NAM']
                row['gdun_final_old'] = matc.loc[city_ind[0][0],'GL_ULT_DUN']
                return row
            else:
                #checking all gduns for that duns name and city
                lis=matc1.GL_ULT_DUN.unique()
                lis1=matc1.GL_ULT_NAM.unique()
                z=''
                x=''
                
                for i in lis:
                    x=x+','+str(i)
                if len(lis)==0:
                    x=''
                else:
                    x=x.split(',',1)[1]
                for i in lis1:
                    z=z +','+str(i)
                if len(lis1)==0:
                    z=''
                else:
                    z=z.split(',',1)[1]
                row['gdun_name_old'] = f'Multiple GDUNS found,{z}'
                row['gdun_final_old'] = f'Multiple GDUNS found,{x}'
                return row
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        if len(matc.GL_ULT_DUN.unique())==1:
            row['gdun_name_old'] = dnb.loc[ind[0][0],'GL_ULT_NAM']
            row['gdun_final_old']=dnb.loc[ind[0][0],'GL_ULT_DUN']
            return row
        else:
            row['gdun_name_old'] = 'Manual'
            row['gdun_final_old'] = '-1'
            return row
    else:
        row['gdun_name_old'] = 'Manual'
        row['gdun_final_old'] = '-1'
        return row

def dnb_c_proccess_old(row,col):
    name = row[col]
    city = row['CITY'] 
    state=row['STATE']
    vec = dnb_vector.transform([name])
    dist,ind = dnb_knn.kneighbors(vec)
    dnb_name=dnb.loc[ind[0][0],'NAME']
    if dist[0][0]<=0.2:
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        matc_vector,matc_knn = create_KNN(matc['STATE'])
        vec = matc_vector.transform([state])
        city_dist,city_ind = matc_knn.kneighbors(vec)
        dnb_city=matc.loc[city_ind[0][0],'STATE']
        if state==matc.loc[city_ind[0][0],'STATE']:
            matc1=matc[matc.STATE==dnb_city]
            matc1.reset_index(inplace=True, drop=True)
            if len(matc1.GL_ULT_DUN.unique())==1:
                row['gdun_name_old'] = matc.loc[city_ind[0][0],'GL_ULT_NAM']
                row['gdun_final_old'] = matc.loc[city_ind[0][0],'GL_ULT_DUN']
                return row
            else:
                lis=matc1.GL_ULT_DUN.unique()
                lis1=matc1.GL_ULT_NAM.unique()
                z=''
                x=''
                
                for i in lis:
                    x=x+','+str(i)
                if len(lis)==0:
                    x=''
                else:
                    x=x.split(',',1)[1]
                for i in lis1:
                    z=z +','+str(i)
                if len(lis1)==0:
                    z=''
                else:
                    z=z.split(',',1)[1]
                
                row['gdun_name_old'] = f'Multiple GDUNS found,{z}'
                row['gdun_final_old'] = f'Multiple GDUNS found,{x}'
                return row
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        if len(matc.GL_ULT_DUN.unique())==1:
            row['gdun_name_old'] = dnb.loc[ind[0][0],'GL_ULT_NAM']
            row['gdun_final_old']=dnb.loc[ind[0][0],'GL_ULT_DUN']
            return row

        else:
            row['gdun_name_old'] = 'Manual'
            row['gdun_final_old'] = '-1'
            return row
    else:
        row['gdun_name_old'] = 'Manual'
        row['gdun_final_old'] = '-1'
        return row

def dnb_proccess_new(row,col):
    name = row[col]
    city = row['CITY']    
    vec = dnb_vector.transform([name])
    dist,ind = dnb_knn.kneighbors(vec)
    dnb_name=dnb.loc[ind[0][0],'NAME']
    #checking the closest match distance is is less that the threshold 0.2
    if dist[0][0]<=0.2:
        #creating a datframe of dnb for the closest match duns name
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        matc_vector,matc_knn = create_KNN(matc['CITY'])
        vec = matc_vector.transform([city])
        city_dist,city_ind = matc_knn.kneighbors(vec)
        dnb_city=matc.loc[city_ind[0][0],'CITY']
	#checking for the matc dataframe if teh city match is less than 0.2
        if city_dist[0][0]<=0.2:
            matc1=matc[matc.CITY==dnb_city]
            matc1.reset_index(inplace=True, drop=True)
            #check if the gdun is unique or not
            if len(matc1.GL_ULT_DUN.unique())==1:
                row['gdun_name_new'] = matc.loc[city_ind[0][0],'GL_ULT_NAM']
                row['gdun_final_new'] = matc.loc[city_ind[0][0],'GL_ULT_DUN']
                return row
            else:
                #checking all gduns for that duns name and city
                lis=matc1.GL_ULT_DUN.unique()
                lis1=matc1.GL_ULT_NAM.unique()
                z=''
                x=''
                
                for i in lis:
                    x=x+','+str(i)
                if len(lis)==0:
                    x=''
                else:
                    x=x.split(',',1)[1]
                for i in lis1:
                    z=z +','+str(i)
                if len(lis1)==0:
                    z=''
                else:
                    z=z.split(',',1)[1]
                row['gdun_name_new'] = f'Multiple GDUNS found,{z}'
                row['gdun_final_new'] = f'Multiple GDUNS found,{x}'
                return row
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        if len(matc.GL_ULT_DUN.unique())==1:
            row['gdun_name_new'] = dnb.loc[ind[0][0],'GL_ULT_NAM']
            row['gdun_final_new']=dnb.loc[ind[0][0],'GL_ULT_DUN']
            return row
        else:
            row['gdun_name_new'] = 'Manual'
            row['gdun_final_new'] = '-1'
            return row
    else:
        row['gdun_name_new'] = 'Manual'
        row['gdun_final_new'] = '-1'
        return row

def dnb_c_proccess_new(row,col):
    name = row[col]
    city = row['CITY'] 
    state=row['STATE']
    vec = dnb_vector.transform([name])
    dist,ind = dnb_knn.kneighbors(vec)
    dnb_name=dnb.loc[ind[0][0],'NAME']
    if dist[0][0]<=0.2:
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        matc_vector,matc_knn = create_KNN(matc['STATE'])
        vec = matc_vector.transform([state])
        city_dist,city_ind = matc_knn.kneighbors(vec)
        dnb_city=matc.loc[city_ind[0][0],'STATE']
        if state==matc.loc[city_ind[0][0],'STATE']:
            matc1=matc[matc.STATE==dnb_city]
            matc1.reset_index(inplace=True, drop=True)
            if len(matc1.GL_ULT_DUN.unique())==1:
                row['gdun_name_new'] = matc.loc[city_ind[0][0],'GL_ULT_NAM']
                row['gdun_final_new'] = matc.loc[city_ind[0][0],'GL_ULT_DUN']
                return row
            else:
                lis=matc1.GL_ULT_DUN.unique()
                lis1=matc1.GL_ULT_NAM.unique()
                z=''
                x=''
                
                for i in lis:
                    x=x+','+str(i)
                if len(lis)==0:
                    x=''
                else:
                    x=x.split(',',1)[1]
                for i in lis1:
                    z=z +','+str(i)
                if len(lis1)==0:
                    z=''
                else:
                    z=z.split(',',1)[1]
                
                row['gdun_name_new'] = f'Multiple GDUNS found,{z}'
                row['gdun_final_new'] = f'Multiple GDUNS found,{x}'
                return row
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        if len(matc.GL_ULT_DUN.unique())==1:
            row['gdun_name_new'] = dnb.loc[ind[0][0],'GL_ULT_NAM']
            row['gdun_final_new']=dnb.loc[ind[0][0],'GL_ULT_DUN']
            return row

        else:
            row['gdun_name_new'] = 'Manual'
            row['gdun_final_new'] = '-1'
            return row
    else:
        row['gdun_name_new'] = 'Manual'
        row['gdun_final_new'] = '-1'
        return row

def mna_proccess(row):
    name = row['NAME']
    cert=row['OLD_ID']
    matc=fdic[fdic.CERT==cert]
    if len(matc)>=1: 
        row=dnb_proccess_old(row,'NAME')
    matc=active[active.NAME==name]
    if len(matc)>=1: 
        row=dnb_c_proccess_old(row,'NAME')  
    else:
        row=dnb_proccess_old(row,'NAME')

    parent_name = row['PARENT_NAME']
    parent_cert=row['NEW_ID']
    matc=fdic[fdic.CERT==parent_cert]
    if len(matc)>=1: 
        row=dnb_proccess_new(row,'PARENT_NAME')
    matc=active[active.NAME==name]
    if len(matc)>=1: 
        row=dnb_c_proccess_new(row,'PARENT_NAME')  
    else:
        row=dnb_proccess_new(row,'PARENT_NAME')
    return row

def cu_proccess(row):
    row=dnb_c_proccess_old(row,'NAME')
    row=dnb_c_proccess_new(row,'PARENT_NAME')
    return row 

if len(title_change)>0:
    df = pd.DataFrame()
    for i in tqdm(range(len(title_change))):
        f_row=title_change.loc[i,:]
        f_row = mna_proccess(f_row)
        df = df.append(f_row)
        print(df)
    df.rename(columns = { 
                        'gdun_name_new' :'NEW_GDUN_NAME',
                        'gdun_final_new':'NEW_GDUN',
                        'gdun_name_old' :'OLD_GDUN_NAME',
                        'gdun_final_old':'OLD_GDUN',
                        'EFFDATE':'MODIFIED_DATE'
                        }, inplace = True)
    temp=df.reindex(columns=['NAME','ORIGINAL_NAME','ORIGINAL_CITY','CITY','STATE','PARENT_NAME','ORIGINAL_PARENT_NAME','ORIGINAL_PARENT_CITY','PARENT_CITY','PARENT_STATE','OLD_ID','NEW_ID','OLD_GDUN_NAME','OLD_GDUN','NEW_GDUN_NAME','NEW_GDUN','SOURCE','REASON','MODIFIED_DATE','APPROVED_IND'])

if len(mna)>0:    
    df1 = pd.DataFrame()
    for i in tqdm(range(len(mna))):
        f_row=mna.loc[i,:]
        f_row = cu_proccess(f_row)
        df1 = df1.append(f_row)
    df1.rename(columns = { 
                        'gdun_name_new' :'NEW_GDUN_NAME',
                        'gdun_final_new':'NEW_GDUN',
                        'gdun_name_old' :'OLD_GDUN_NAME',
                        'gdun_final_old':'OLD_GDUN',
                        'DATE_':'MODIFIED_DATE'
                        }, inplace = True)
    temp2=df2.reindex(columns=['NAME','ORIGINAL_NAME','ORIGINAL_CITY','CITY','STATE','PARENT_NAME','ORIGINAL_PARENT_NAME','ORIGINAL_PARENT_CITY','PARENT_CITY','PARENT_STATE','OLD_ID','NEW_ID','OLD_GDUN_NAME','OLD_GDUN','NEW_GDUN_NAME','NEW_GDUN','SOURCE','REASON','MODIFIED_DATE','APPROVED_IND'])
lis=pd.DataFrame()
if len(title_change)>0:
    lis=lis.append(temp)
if len(mna)>0:
    lis=lis.append(temp2)
if len(lis)==0:
    exit()
print(lis)
lis.fillna('null',inplace=True)
lis=lis.astype(str)
lis.reset_index(inplace=True, drop=True)
i=0
while i <len(lis):
    valu=tuple(lis.iloc[i])
    print(len(valu))
    valu = tuple([val.replace("'","''") if isinstance(val, str) else val for val in valu])
    valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])

    valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])
    print(valu)



    print(valu)
    a=valu[18]

    valuw = valu[:18] + (f"to_timestamp('{a}','YYYY-MM-DD HH24:MI:SS')",)
    valuws=valuw+(valu[19],)
    valuws=str(valuws)
    valus=valuws.replace("'null'","NULL") 
    valus=valus.replace('\'"',"'")
    valus=valus.replace('"\'',"'")
    valus=valus.replace('"',"'")

    

    valus=valus.replace("'to_timestamp",'TO_TIMESTAMP')
    valus=valus.replace("MM-DD HH24:MI:SS')'","MM-DD HH24:MI:SS')")
    valus=valus.replace('"',"'")
    
    print(valus)


    insert_query = f"""INSERT INTO mstrstg.ongoing_mna (NAME,ORIGINAL_NAME,ORIGINAL_CITY,CITY,STATE,PARENT_NAME,ORIGINAL_PARENT_NAME,ORIGINAL_PARENT_CITY,PARENT_CITY,PARENT_STATE,OLD_ID,NEW_ID,OLD_GDUN_NAME,OLD_GDUN,NEW_GDUN_NAME,NEW_GDUN,SOURCE,REASON,MODIFIED_DATE,APPROVED_IND ) VALUES {valus}"""


# execute the INSERT query with the values
    cursor.execute(insert_query)

# commit the transaction
    conn.commit()
    i=i+1

# close the cursor and connection objects
cursor.close()
conn.close()