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
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)
from itertools import chain
import json
import csv

#creating a log file
log_file_path = '/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Logs/wk_file.log'
fh = logging.FileHandler(log_file_path, delay=True)  # set delay to True
fh.setLevel(logging.DEBUG)

# create formatter and add it to the file handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

# add the file handler to the logger
logger.addHandler(fh)

# log messages
logger.debug('This is a debug message')
logger.info('This is an info message')
logger.warning('This is a warning message')
logger.error('This is an error message')
logger.critical('This is a critical message')

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


#reading UCC data
cur_run_query= connection_data['ucc_query']
#reading the sql query results in a dataframe
current_run=pd.read_sql(cur_run_query, con=conn)
print(current_run)

sq4=f"""select distinct  duns_num, company, city, st, gl_ult_dun, gl_ult_nam from core.wk_fullfile
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
print(dnb)

fdic_query="""select fdic_id CERT,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,MAINOFF,STATE, SOURCE FROM mstrstg.FDIC_active_FILE"""
ac=pd.read_sql(fdic_query,con=conn)
ac['MAINOFF'] = ac['MAINOFF'].astype(int)
df1=pd.read_excel('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Input/FDIC_IDS.xlsx',engine='openpyxl')

list_of_id=df1.OUT_CERT.unique()
fdic = pd.DataFrame()
for i in tqdm(range(len(ac))):
    fr_row = ac.loc[i,:]
    a=fr_row['CERT']
    if a not in list_of_id:
        fdic=fdic.append(fr_row)
fdic.reset_index(inplace=True, drop=True)
fdic_events_query="""select fdic_id OUT_CERT,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE, PARENT_FDIC_ID SUR_CERT, COMPRESSES_PARENT_NAME PARENT_NAME, COMPRESSES_PARENT_CITY PARENT_CITY, PARENT_STATE, SOURCE FROM mstrstg.FDIC_events_FILE"""
title_change=pd.read_sql(fdic_events_query,con=conn)
credit_union_active_query="""select NCUA_ID ID_NCUA,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE, SOURCE FROM mstrstg.CREDIT_UNION_ACTIVE"""
active=pd.read_sql(credit_union_active_query,con=conn)
credit_union_closed_query="""select NCUA_ID ID_NCUA,COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE, SOURCE FROM mstrstg.CREDIT_UNION_CLOSED"""
closed=pd.read_sql(credit_union_closed_query,con=conn)
credit_union_mna_query="""SELECT OLD_NCUA_ID OLD_ID, NEW_NCUA_ID NEW_ID, COMPRESSED_NAME NAME, COMPRESSED_CITY CITY,STATE,COMPRESSED_PARENT_NAME PARENT_NAME,COMPRESSED_PARENT_CITY PARENT_CITY,PARENT_STATE,SOURCE FROM mstrstg.CREDIT_UNION_MNA"""
mna=pd.read_sql(credit_union_mna_query,con=conn)
city_st=pd.read_excel('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Input/WK_City_state.xlsx',engine='openpyxl')

def preprocess_text(sentence):
    #remove brackets from the string
    try:
        sentence=re.sub("\(.*?\)", "", sentence)
    except:
        print("")
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
    #remove brackets from the string
    try:
        sen=re.sub("\(.*?\)", "", sen)
    except:
        print("")
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
            ' D/B/A',
            ' loan operations',
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
            ' National association',
            ' National associatio',
            ' National associati',
            ' Nationl association',
            ' Natonal association',
            ' Natoinal association',
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

def wkls_list_naming(df):
    df.rename(columns = {'SEC_PARTY_CMN_NM':'NAME','SEC_PARTY_ADDR_CITY_NM':'CITY','SEC_PARTY_ADDR_STATE_CD':'STATE'}, inplace = True)
    df['Orginal_Name'] = df['NAME']
    df['Orginal_CITY'] = df['CITY']
    df['Orginal_STATE'] = df['STATE']
    df['NAME'] = df['NAME'].apply(filter_word)
    df['NAME']=df['NAME'].apply(preprocess_text)    
    df['CITY']=df['CITY'].apply(preprocess_text)
    df['CITY']=df['CITY'].apply(city_change)
    words_to_filter = [' llc', ' llp', ' limited liability partnership', ' limited liability company', ' l.l.c', ' l.l.c.', ' ll.c.', ' inc', ' corp.', ' inc.', ' corp', ' ltd', ' ltd.', ' llc.', ' co.', ' co', ' lp', ' lp.', ' limited liability co.', ' limited liability co', ' limited liability compa', ' limited liability comp',',llc', ',llp', ',limited liability partnership', ',limited liability company', ',l.l.c', ',l.l.c.', ',ll.c.', ',inc', ',corp.', ',inc.', ',corp', ',ltd', ',ltd.', ',llc.', ',co.', ',co', ',lp', ',lp.', ',limited liability co.', ',limited liability co', ',limited liability compa', ',limited liability comp']
    df1 = df[~((df['Orginal_Name'].str.lower().str.endswith(tuple(words_to_filter)) & ~(df['Orginal_Name'].str.lower().str.contains('bank|credit union'))) | ((df['Orginal_Name'].str.lower().str.startswith('city of') & ~(df['Orginal_Name'].str.lower().str.contains('bank|credit union')))))]
    df2 = df[((df['Orginal_Name'].str.lower().str.endswith(tuple(words_to_filter)) & ~(df['Orginal_Name'].str.lower().str.contains('bank|credit union'))) | ((df['Orginal_Name'].str.lower().str.startswith('city of') & ~(df['Orginal_Name'].str.lower().str.contains('bank|credit union')))))]

    print(df1,df2)
    return df1,df2
    
def dnb_list_naming(dnb):
    dnb.rename(columns = {'COMPANY':'NAME','CITY':'CITY','ST':'STATE'}, inplace = True)
    dnb['NAME'] = dnb['NAME'].apply(filter_word)
    dnb['NAME']=dnb['NAME'].apply(preprocess_text)    
    dnb['CITY']=dnb['CITY'].apply(preprocess_text)
    dnb['CITY']=dnb['CITY'].apply(city_change)
    return dnb


def city_state(df):
    df.rename(columns = {'city_ascii':'CITY','state_id':'STATE'}, inplace = True) 
    df['CITY']=df['CITY'].apply(preprocess_text)
    df['CITY']=df['CITY'].apply(city_change)
    return df
    
city_st=city_state(city_st)
city_st.reset_index(inplace=True, drop=True)
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

#Creating the knn for all the dataframes
fdic_vector,fdic_knn = create_KNN(fdic['NAME'])
title_change_vector,title_change_knn = create_KNN(title_change['NAME'])
cs_vector,cs_knn = create_KNN(city_st['CITY'])
active_vector,active_knn = create_KNN(active['NAME'])
closed_vector,closed_knn = create_KNN(closed['NAME'])
mna_vector,mna_knn = create_KNN(mna['NAME'])
dnb.reset_index(inplace = True,drop = True)
dnb_vector,dnb_knn = create_KNN(dnb['NAME'])

#cleaning the ucc data
current_run, current_run1 = wkls_list_naming(current_run)
current_run.reset_index(inplace=True, drop=True)
current_run1.reset_index(inplace=True, drop=True)


current_run['STATE'] = current_run['STATE'].replace(np.nan, " ")
current_run1['STATE'] = current_run1['STATE'].replace(np.nan, " ")
current_run['CITY'] = current_run['CITY'].replace(np.nan, " ")
current_run1['CITY'] = current_run1['CITY'].replace(np.nan, " ")
fdic['STATE']=fdic['STATE'].replace(np.nan, " ")
active['STATE']=active['STATE'].replace(np.nan, " ")
title_change['STATE']=title_change['STATE'].replace(np.nan, " ")
title_change['CITY']=title_change['CITY'].replace(np.nan, " ")
title_change['PARENT_CITY']=title_change['PARENT_CITY'].replace(np.nan, " ")


#dnb proccess 
def dnb_proccess(row,col,inp):
    name = row[col]
    city = row['CITY']    
    vec = dnb_vector.transform([name])
    dist,ind = dnb_knn.kneighbors(vec)
    dnb_name=dnb.loc[ind[0][0],'NAME']
    row['dnb_dist']=dist[0][0]
    #checking the closest match distance is is less that the threshold 0.2
    if dist[0][0]<=0.2:
        #creating a datframe of dnb for the closest match duns name
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        matc_vector,matc_knn = create_KNN(matc['CITY'])
        vec = matc_vector.transform([city])
        city_dist,city_ind = matc_knn.kneighbors(vec)
        row['dnb_city_distance'] = city_dist[0][0]
        dnb_city=matc.loc[city_ind[0][0],'CITY']
	#checking for the matc dataframe if teh city match is less than 0.2
        if city_dist[0][0]<=0.2:
            matc1=matc[matc.CITY==dnb_city]
            matc1.reset_index(inplace=True, drop=True)
            #check if the gdun is unique or not
            if len(matc1.GL_ULT_DUN.unique())==1:
                row['dnb_city']=matc.loc[city_ind[0][0],'CITY']
                row['dun_num']=matc.loc[city_ind[0][0],'DUNS_NUM']
                row['dnb_name'] = matc.loc[city_ind[0][0],'NAME']
                row['dnb_unique_Count']=0
                row['gdun_name'] = matc.loc[city_ind[0][0],'GL_ULT_NAM']
                row['gdun_final'] = matc.loc[city_ind[0][0],'GL_ULT_DUN']
                row['DUNS_count']=len(matc1.DUNS_NUM.unique())
                if len(matc1.DUNS_NUM.unique())<=100:
                    list_of_dun=matc1.DUNS_NUM.unique()
                else:
                    list_of_d=matc1.DUNS_NUM.unique()
                    list_of_dun=list_of_d[0:101]

                dunli=''
                for i in list_of_dun:
                    dunli=dunli+','+str(i)
                row['list_of_duns']=dunli
                if row['DUNS_count']==1:
                    row['dun_num']=matc.loc[city_ind[0][0],'DUNS_NUM']
                else:
                    row['dun_num']=-1
                row['Comments']='Data from DNB'
                try:
                    a=row['OLD_GDUN'][0]
                    if  a is not None:
                        if row['OLD_GDUN']==row['gdun_final']:
                            row['match_ind']='Y'    
                        else:
                            row['match_ind']='N'
                except:
                    print("")
                if "SOURCE" not in row.index:
                    row['SOURCE']='DNB'
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
                row['dnb_city']=matc.loc[city_ind[0][0],'CITY']
                row['dnb_name'] = matc.loc[city_ind[0][0],'NAME']
                row['dnb_unique_Count']=0
                if len(matc1.DUNS_NUM.unique())<=100:
                    list_of_dun=matc1.DUNS_NUM.unique()
                else:
                    list_of_d=matc1.DUNS_NUM.unique()
                    list_of_dun=list_of_d[0:101]

                dunli=''
                for i in list_of_dun:
                    dunli=dunli+','+str(i)
                row['list_of_duns']=dunli
                row['gdun_name'] = f'Multiple GDUNS found,{z}'
                row['gdun_final'] = -67000
                
                row['DUNS_count']=len(matc1.DUNS_NUM.unique())
                row['Comments']=f'Multiple GDUNS found,{x}'
                if "SOURCE" not in row.index:
                    row['SOURCE']='DNB'
                row['dun_num']=-1
                return row
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        if len(matc.GL_ULT_DUN.unique())==1:
            row['dnb_name']=dnb.loc[ind[0][0],'NAME']
            row['gdun_name'] = dnb.loc[ind[0][0],'GL_ULT_NAM']
            row['dnb_unique_Count']=1
            row['gdun_final']=dnb.loc[ind[0][0],'GL_ULT_DUN']
            if len(matc.DUNS_NUM.unique())<=100:
                list_of_dun=matc.DUNS_NUM.unique()
            else:
                list_of_d=matc.DUNS_NUM.unique()
                list_of_dun=list_of_d[0:101]

            dunli=''
            for i in list_of_dun:
                dunli=dunli+','+str(i)
            row['list_of_duns']=dunli
            row['DUNS_count']=len(matc.DUNS_NUM.unique())
            if row['DUNS_count']==1:
                row['dun_num']=dnb.loc[ind[0][0],'DUNS_NUM']
            else:
                row['dun_num']=-1
            row['Comments']='unique gdun for assigned duns name'
            try:
                a=row['OLD_GDUN'][0]
                if  a is not None:
                    if row['OLD_GDUN']==row['gdun_final']:
                        row['match_ind']='Y'    
                    else:
                        row['match_ind']='N'
            except:
                 print("")
            if "SOURCE" not in row.index:
                row['SOURCE']='DNB'
            return row
        if "HQ" in row.index:
            print('hq')
            matc_vector,matc_knn = create_KNN(matc['CITY'])
            vec = matc_vector.transform([row['HQ']])
            hq_dist,hq_ind = matc_knn.kneighbors(vec)
            row['dnb_hq_distance'] = hq_dist[0][0]
            dnb_hq=matc.loc[hq_ind[0][0],'CITY']
            if hq_dist[0][0]<=0.2:
                matc1=matc[matc.CITY==dnb_hq]
                matc1.reset_index(inplace=True, drop=True)
                if len(matc1.GL_ULT_DUN.unique())==1:
                    row['gdun_name'] = matc.loc[hq_ind[0][0],'GL_ULT_NAM']
                    
                    row['dnb_name'] = matc.loc[hq_ind[0][0],'NAME']
                    row['gdun_final'] = matc.loc[hq_ind[0][0],'GL_ULT_DUN']
                    row['dnb_unique_Count']=0
                    row['DUNS_count']=len(matc1.DUNS_NUM.unique())
                    if len(matc1.DUNS_NUM.unique())<=100:
                        list_of_dun=matc1.DUNS_NUM.unique()
                    else:
                        list_of_d=matc1.DUNS_NUM.unique()
                        list_of_dun=list_of_d[0:101]

                    dunli=''
                    for i in list_of_dun:
                        dunli=dunli+','+str(i)
                    row['list_of_duns']=dunli
                    if row['DUNS_count']==1:
                        row['dun_num']=matc.loc[hq_ind[0][0],'DUNS_NUM']
                    else:
                        row['dun_num']=-1
                    
                    row['Comments']='Data from DNB using Headquaters'
                    try:
                        a=row['OLD_GDUN'][0]
                        if  a is not None:
                            if row['OLD_GDUN']==row['gdun_final']:
                                row['match_ind']='Y'    
                            else:
                                row['match_ind']='N'
                    except:
                        print("")
                    if "SOURCE" not in row.index:
                        row['SOURCE']='DNB'                   
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
                    row['dnb_name'] = matc.loc[hq_ind[0][0],'NAME']
                    row['dnb_unique_Count']=0
                    row['gdun_name'] = f'Multiple GDUNS found,{z}'
                    row['gdun_final'] = -67000
                    row['DUNS_count']=len(matc1.DUNS_NUM.unique())
                    row['dun_num']=-1
                    if len(matc1.DUNS_NUM.unique())<=100:
                        list_of_dun=matc1.DUNS_NUM.unique()
                    else:
                        list_of_d=matc1.DUNS_NUM.unique()
                        list_of_dun=list_of_d[0:101]

                    dunli=''
                    for i in list_of_dun:
                        dunli=dunli+','+str(i)
                    row['list_of_duns']=dunli
                    row['Comments']=f'Multiple GDUNS found for headquaters,{x}'
                    
                    if "SOURCE" not in row.index:
                        row['SOURCE']='DNB'
                    return row
                
                
                
            else:
                row['gdun_name'] = 'Manual'
                row['Comments']=inp
                row['dnb_name'] = 'Manual'
                row['gdun_final'] = -1
                row['Comments']=inp
                row['dun_num']=-1
                row['DUNS_count']=-1
                row['dnb_unique_Count']=0
                return row
        else:
            row['gdun_name'] = 'Manual'
            row['dnb_name'] = 'Manual'
            row['gdun_final'] = -1
            row['DUNS_count']=-1
            row['dun_num']=-1
            row['Comments']=inp
            row['dnb_unique_Count']=0
            return row
    else:
        row['gdun_name'] = 'Manual'
        row['dnb_name'] = 'Manual'
        row['gdun_final'] = -1
        row['DUNS_count']=-1
        row['dun_num']=-1
        row['Comments']=inp
        row['dnb_unique_Count']=0
        return row

def dnb_c_proccess(row,col,inp):
    name = row[col]
    city = row['CITY'] 
    state=row['STATE']
    vec = dnb_vector.transform([name])
    dist,ind = dnb_knn.kneighbors(vec)
    dnb_name=dnb.loc[ind[0][0],'NAME']
    row['dnb_dist']=dist[0][0]
    if dist[0][0]<=0.2:
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        matc_vector,matc_knn = create_KNN(matc['STATE'])
        vec = matc_vector.transform([state])
        city_dist,city_ind = matc_knn.kneighbors(vec)
        row['dnb_state_distance'] = city_dist[0][0]
        dnb_city=matc.loc[city_ind[0][0],'STATE']
        if state==matc.loc[city_ind[0][0],'STATE']:
            matc1=matc[matc.STATE==dnb_city]
            matc1.reset_index(inplace=True, drop=True)
            if len(matc1.GL_ULT_DUN.unique())==1:
                row['dnb_state']=matc.loc[city_ind[0][0],'STATE']
                row['dun_num']=matc.loc[city_ind[0][0],'DUNS_NUM']
                row['dnb_name'] = matc.loc[city_ind[0][0],'NAME']
                row['dnb_unique_Count']=0
                row['gdun_name'] = matc.loc[city_ind[0][0],'GL_ULT_NAM']
                row['gdun_final'] = matc.loc[city_ind[0][0],'GL_ULT_DUN']
                row['DUNS_count']=len(matc1.DUNS_NUM.unique())
                if len(matc1.DUNS_NUM.unique())<=100:
                    list_of_dun=matc1.DUNS_NUM.unique()
                else:
                    list_of_d=matc1.DUNS_NUM.unique()
                    list_of_dun=list_of_d[0:101]

                dunli=''
                for i in list_of_dun:
                    dunli=dunli+','+str(i)
                row['list_of_duns']=dunli
                if row['DUNS_count']==1:
                    row['dun_num']=matc.loc[city_ind[0][0],'DUNS_NUM']
                else:
                    row['dun_num']=-1
                
                row['Comments']='Data from DNB'
                try:
                    a=row['OLD_GDUN'][0]
                    if  a is not None:
                        if row['OLD_GDUN']==row['gdun_final']:
                            row['match_ind']='Y'    
                        else:
                            row['match_ind']='N'
                except:
                    print("")
                if "SOURCE" not in row.index:
                    row['SOURCE']='DNB'
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
                
                row['dnb_state']=matc.loc[city_ind[0][0],'STATE']
                row['dnb_name'] = matc.loc[city_ind[0][0],'NAME']
                row['dnb_unique_Count']=0
                row['gdun_name'] = f'Multiple GDUNS found,{z}'
                row['gdun_final'] = -67000
                if len(matc1.DUNS_NUM.unique())<=100:
                    list_of_dun=matc1.DUNS_NUM.unique()
                else:
                    list_of_d=matc1.DUNS_NUM.unique()
                    list_of_dun=list_of_d[0:101]

                dunli=''
                for i in list_of_dun:
                    dunli=dunli+','+str(i)
                row['list_of_duns']=dunli
                row['DUNS_count']=len(matc1.DUNS_NUM.unique())
                row['Comments']=f'Multiple GDUNS found,{x}'

                if "SOURCE" not in row.index:
                    row['SOURCE']='DNB'
                row['dun_num']=-1
                return row
        matc=dnb[dnb.NAME==dnb_name]
        matc.reset_index(inplace=True, drop=True)
        if len(matc.GL_ULT_DUN.unique())==1:
            row['dnb_name']=dnb.loc[ind[0][0],'NAME']
            row['gdun_name'] = dnb.loc[ind[0][0],'GL_ULT_NAM']
            row['dnb_unique_Count']=1
            row['Comments']='unique gdun for assigned duns name'
            row['gdun_final']=dnb.loc[ind[0][0],'GL_ULT_DUN']
            if len(matc.DUNS_NUM.unique())<=100:
                list_of_dun=matc.DUNS_NUM.unique()
            else:
                list_of_d=matc.DUNS_NUM.unique()
                list_of_dun=list_of_d[0:101]

            dunli=''
            for i in list_of_dun:
                dunli=dunli+','+str(i)
            row['list_of_duns']=dunli
            row['DUNS_count']=len(matc.DUNS_NUM.unique())
            if row['DUNS_count']==1:
                row['dun_num']=dnb.loc[ind[0][0],'DUNS_NUM']
            else:
                row['dun_num']=-1
            
            try:
                a=row['OLD_GDUN'][0]
                if  a is not None:
                    if row['OLD_GDUN']==row['gdun_final']:
                        row['match_ind']='Y'    
                    else:
                        row['match_ind']='N'
            except:
                 print("")
            if "SOURCE" not in row.index:
                row['SOURCE']='DNB'
            return row

        else:
            row['gdun_name'] = 'Manual'
            row['dnb_name'] = 'Manual'
            row['gdun_final'] = -1
            row['DUNS_count']=-1
            row['dun_num']=-1
            row['Comments']=inp
            row['dnb_unique_Count']=0
            return row
    else:
        row['gdun_name'] = 'Manual'
        row['dnb_name'] = 'Manual'
        row['gdun_final'] = -1
        row['DUNS_count']=-1
        row['dun_num']=-1
        row['Comments']=inp
        row['dnb_unique_Count']=0
        return row


def credit_proccess(row,col):
    print(row['Orginal_Name'])
    o_name = str(row['Orginal_Name']).lower()
    print(o_name)
    if o_name[-3:]==' cu' or o_name[-4:]==' fcu' or o_name[-3:]==',cu' or o_name[-4:]==',fcu' or  o_name[-6:]==' union' in o_name:
        print('y')
        name = row[col]
    
        city = row['CITY']
        state = row['STATE']
        vec = active_vector.transform([name])
        dist,ind = active_knn.kneighbors(vec)
        row['active_credit_dist'] = dist[0][0]
        active_name=active.loc[ind[0][0],'NAME']
        if dist[0][0] <= 0.2:
            matc=active[active.NAME==active_name]
            matc.reset_index(inplace=True, drop=True)
            matc_vector,matc_knn = create_KNN(matc['STATE'])
            vec = matc_vector.transform([state])
            state_dist,state_ind = matc_knn.kneighbors(vec)
            state_match = matc.loc[state_ind[0][0],'STATE']
            if len(matc.ID_NCUA.unique())==1:
                row['Credit_final_parent'] = matc.loc[state_ind[0][0],'NAME']
                row['active_credit_name'] = matc.loc[state_ind[0][0],'NAME']
                row['SOURCE']='CREDITUNION_ACTIVE'
                row=dnb_c_proccess(row,'active_credit_name','Data from credit unions active (unique id)')
                return row
            temp=matc[matc.STATE==state]
            temp.reset_index(inplace=True, drop=True)
            if len(temp.ID_NCUA.unique())==1:
                row['Credit_final_parent'] = matc.loc[state_ind[0][0],'NAME']
                row['SOURCE']='CREDITUNION_ACTIVE'
                row['active_name'] = matc.loc[state_ind[0][0],'NAME']
                row['active_credit_name'] = matc.loc[state_ind[0][0],'NAME']
                row=dnb_c_proccess(row,'active_credit_name','Data from credit unions active (unique state, multiple ids)')
                return row
            else:
                row['SOURCE']='CREDITUNION_ACTIVE'
                row['Credit_final_parent'] = matc.loc[state_ind[0][0],'NAME']
                row['active_credit_name'] = matc.loc[state_ind[0][0],'NAME']
                row['detailed_info']= 'credit unions active multiple state, multiple ids found'
                row['active_name'] = matc.loc[state_ind[0][0],'NAME']

                row=dnb_c_proccess(row,'active_credit_name','Data from credit unions active (multiple state, multiple ids)')
                return row

        vec = mna_vector.transform([name])
        dist,ind = mna_knn.kneighbors(vec)
        row['title_mna_dist'] = dist[0][0]
        child_name=mna.loc[ind[0][0],'NAME']

        if dist[0][0] <= 0.2:

            matc=mna[mna.NAME==child_name]
            matc.reset_index(inplace=True, drop=True)   
            matc_vector,matc_knn = create_KNN(matc['STATE'])
            vec = matc_vector.transform([state])
            state_dist,state_ind = matc_knn.kneighbors(vec)
            state_match = matc.loc[state_ind[0][0],'STATE']
            if len(matc.OLD_ID.unique())==1:
                row['SOURCE']='CREDITUNION_MNA'
                row['Credit_final_parent'] = matc.loc[state_ind[0][0],'NAME']
                row['credit_parent_name'] = matc.loc[state_ind[0][0],'PARENT_NAME']
                row=dnb_c_proccess(row,'credit_parent_name','Data from credit unions MNA (unique id)')
                return row
            temp=matc[matc.STATE==state]
            temp.reset_index(inplace=True, drop=True)
            if len(temp.OLD_ID.unique())==1:
                row['SOURCE']='CREDITUNION_MNA' 
                row['Credit_final_parent'] = matc.loc[state_ind[0][0],'NAME']
                row['credit_parent_name'] = matc.loc[state_ind[0][0],'PARENT_NAME']
                row=dnb_c_proccess(row,'credit_parent_name','Data from credit unions MNA (unique state, multiple ids)')
                return row
            else:
                row['SOURCE']='CREDITUNION_MNA'
                row['Credit_final_parent'] = matc.loc[state_ind[0][0],'NAME']
                row['credit_parent_name'] = matc.loc[state_ind[0][0],'PARENT_NAME']
                row['detailed_info']= 'credit unions mna multiple state, multiple ids found'
                row=dnb_c_proccess(row,'credit_parent_name','Data from credit unions MNA (unique state, multiple ids)')
                return row
        vec = closed_vector.transform([name])
        dist,ind = closed_knn.kneighbors(vec)
        row['closed_dist'] = dist[0][0]
        closed_name=closed.loc[ind[0][0],'NAME']
        if dist[0][0] <= 0.2:
            matc=closed[closed.NAME==closed_name]
            matc.reset_index(inplace=True, drop=True)
            matc_vector,matc_knn = create_KNN(matc['STATE'])
            vec = matc_vector.transform([state])
            state_dist,state_ind = matc_knn.kneighbors(vec)
            state_match = matc.loc[state_ind[0][0],'STATE']
            if len(matc.ID_NCUA.unique())==1:
                row['closed_name'] = matc.loc[state_ind[0][0],'NAME']
                row['SOURCE']='CREDITUNION_CLOSED'
                row['dnb_name'] = 'manual'
                row['detailed_info']= 'credit union Liquidates'
                row['gdun_final'] = -21000
                row['gdun_name'] = 'Manual'
                row['dun_num']=-1
                row['match_ind']=''  
                row['Comments']='Data from credit unions closed (unique id)'
                return row
            temp=matc[matc.STATE==state]
            temp.reset_index(inplace=True, drop=True)
            if len(matc.ID_NCUA.unique())==1:
                row['SOURCE']='CREDITUNION_CLOSED'
                row['closed_name'] = matc.loc[state_ind[0][0],'NAME']
                row['dnb_name'] = 'manual'
                row['detailed_info']= 'credit union Liquidates'
                row['gdun_final'] = -21000
                row['dun_num']=-1
                row['match_ind']=''  
                row['gdun_name'] = 'Manual'
                row['Comments']='Data from credit unions closed (unique id, multiple state)'
                return row  
            else:
                row['closed_name'] = matc.loc[state_ind[0][0],'NAME']
                row['SOURCE']='CREDITUNION_CLOSED'
                row['dnb_name'] = 'manual'
                row['dun_num']=-1
                row['match_ind']=''  
                row['detailed_info']= 'credit union Liquidates'
                row['Comments']='Data from credit unions closed (multiple id, multiple state)'
                row['gdun_final'] = -21000
                row['gdun_name'] = 'Manual'
                return row
        else:
            row=dnb_proccess(row,'NAME','')
            return row

    else:
        row=dnb_proccess(row,'NAME','')
        return row
    return row

def active_proccess(row,col):
    name = row[col]
    city = row['CITY']
    state = row['STATE']
    vec = fdic_vector.transform([name])
    dist,ind = fdic_knn.kneighbors(vec)
    row['active_dist'] = dist[0][0]
    active_name=fdic.loc[ind[0][0],'NAME']
    if dist[0][0] <= 0.2:
        matc=fdic[fdic.NAME==active_name]        
        matc.reset_index(inplace=True, drop=True)
        matc_vector,matc_knn = create_KNN(matc['CITY'])
        vec = matc_vector.transform([city])
        city_dist,city_ind = matc_knn.kneighbors(vec)
        active_city=matc.loc[city_ind[0][0],'CITY']
        row['active_city_dist'] = city_dist[0][0]       
        if city_dist[0][0]<=0.2:
            matc1=matc[matc.CITY==active_city]
            matc1.reset_index(inplace=True, drop=True)
            matc1_vector,matc1_knn = create_KNN(matc1['STATE'])
            vec = matc1_vector.transform([state])
            state_dist,state_ind = matc1_knn.kneighbors(vec)
            if len(matc.CERT.unique())==1:
                row['active_unique_count']=1
            else:
                row['active_unique_count']=0
            if len(matc1)==1:
                a0=matc1.CERT.unique()
                a1=a0[0]
                a2=matc[matc['CERT'] ==a1 ]
                if len(a2[a2['MAINOFF'] == 1]) > 0:          
                    row['HQ'] = list(a2[a2['MAINOFF'] == 1]['CITY'])[0]
                row['active_name'] = matc.loc[city_ind[0][0],'NAME']
                row['active_city'] = matc.loc[city_ind[0][0],'CITY'] 
                row['final active Name'] = matc.loc[city_ind[0][0],'NAME']
                row['final active city'] = matc.loc[city_ind[0][0],'CITY']
                row['final active state'] = matc.loc[city_ind[0][0],'STATE']
                row['SOURCE'] = matc.loc[city_ind[0][0],'SOURCE']
                row = dnb_proccess(row,'active_name','data from FDIC active file(name and city) with unique FDIC_ID')
                return row
                
            elif len(matc1.CERT.unique())==1:
                a0=matc1.CERT.unique()
                a1=a0[0]
                a2=matc[matc['CERT'] ==a1 ]
                if len(a2[a2['MAINOFF'] == 1]) > 0:          
                    row['HQ'] = list(a2[a2['MAINOFF'] == 1]['CITY'])[0]
                row['active_name'] = matc1.loc[state_ind[0][0],'NAME']
                row['active_city'] = matc1.loc[state_ind[0][0],'CITY'] 
                row['final active Name'] = matc1.loc[state_ind[0][0],'NAME']
                row['final active city'] = matc1.loc[state_ind[0][0],'CITY']
                row['final active state'] = matc1.loc[state_ind[0][0],'STATE']
                row['SOURCE'] = matc1.loc[state_ind[0][0],'SOURCE']
                row = dnb_proccess(row,'active_name','data from FDIC active file(name and city) with unique FDIC_ID')
                return row
            else:
                state_match = matc1.loc[state_ind[0][0],'STATE']
                matc2=matc1[matc1.STATE==state_match]
                matc2.reset_index(inplace=True, drop=True)
                if len(matc2.CERT.unique())==1:
                    a0=matc2.CERT.unique()
                    a1=a0[0]
                    a2=matc[matc['CERT'] ==a1 ]
                    if len(a2[a2['MAINOFF'] == 1]) > 0:          
                        row['HQ'] = list(a2[a2['MAINOFF'] == 1]['CITY'])[0]
                    row['active_name'] = matc1.loc[state_ind[0][0],'NAME']
                    row['active_city'] = matc1.loc[state_ind[0][0],'CITY'] 
                    row['final active Name'] = matc1.loc[state_ind[0][0],'NAME']
                    row['final active city'] = matc1.loc[state_ind[0][0],'CITY']
                    row['final active state'] = matc1.loc[state_ind[0][0],'STATE']
                    row['SOURCE'] = matc1.loc[state_ind[0][0],'SOURCE']
                    row = dnb_proccess(row,'active_name','data from active file (name , city and state), multiple parents from fdic active with same name and city')
                    return row
                else:
                    row['active_name'] = matc1.loc[state_ind[0][0],'NAME']
                    row['active_city'] = matc1.loc[state_ind[0][0],'CITY'] 
                    row['final active Name'] = matc1.loc[state_ind[0][0],'NAME']
                    row['final active city'] = matc1.loc[state_ind[0][0],'CITY']
                    row['final active state'] = matc1.loc[state_ind[0][0],'STATE']
                    row['SOURCE'] = matc1.loc[state_ind[0][0],'SOURCE']
                    row = dnb_proccess(row,'active_name','data from active file (name , city and state) and multiple ids, multiple parents from fdic active with same name and city')
                    return row
            

        else:
            row['HQ']=''
            vec = title_change_vector.transform([name])
            distance,index = title_change_knn.kneighbors(vec)
            row['title_distance'] = distance[0][0]
            change_name=title_change.loc[index[0][0],'NAME']
            if distance[0][0] <= 0.2:
                matc22=title_change[title_change.NAME==change_name]
                matc22.reset_index(inplace = True,drop = True)
                list_1=matc22.PARENT_NAME.unique()
                if len(list_1)==0:
                    row = credit_proccess(row,'NAME','data from active file (name)')
                    return row
                if len(list_1)==1:
                    row['parent_unique_count']=1
                else:
                    row['parent_unique_count']=0
                y=''
                co=0
                x=''
                g=''
                kre=0
                yu=''
                list_of_vari=pd.DataFrame()
                for i in list_1:
                    row['parent'] = i
                    x=x+","+i
                    
                    vec = fdic_vector.transform([row['parent']])
                    distance,index = fdic_knn.kneighbors(vec)
                    p_ac_dis = distance[0][0]
                    some_match = fdic.loc[index[0][0],'NAME']
                    row['SOURCE']=matc22.loc[matc22.PARENT_NAME==i,'SOURCE'].values[0]
                    if distance[0][0] <= 0.2:
                        temp = fdic[fdic.NAME == some_match]
                        temp.reset_index(inplace = True,drop = True)
                        if len(temp) == 0:
                            pass
                        
                        temp_vector,temp_knn = create_KNN(temp['STATE'])
                        vec = temp_vector.transform([state])
                        distance,index = temp_knn.kneighbors(vec)
                        p_c_d = distance[0][0]
                        state_match = temp.loc[index[0][0],'STATE']
                        if distance==0:
                            matc=temp[temp.STATE==state_match]
                            matc.reset_index(inplace=True, drop=True)
                            if len(matc) == 0:
                                pass
                            kre=kre+1
                            matc_vector,matc_knn = create_KNN(matc['CITY'])

                            vec = matc_vector.transform([city])
                            distance,index = matc_knn.kneighbors(vec)
                            row['ignore_p'] =i
                            row['pa']=p_ac_dis
                            city_match = matc.loc[index[0][0],'CITY']
                            if distance[0][0] <= 0.2:                                
                                y=y+","+i
                                co=co+1
                                matc34=matc[matc.CITY==city_match]
                                matc34.reset_index(inplace=True, drop=True)
                                a0=matc34.CERT.unique()
                                a1=a0[0]
                                a2=temp[temp['CERT'] ==a1 ]
                                if len(a2[a2['MAINOFF'] == 1]) ==1:          
                                    row['HQ'] = list(a2[a2['MAINOFF'] == 1]['CITY'])[0]
                                row['Parent_active_distance'] = p_ac_dis
                                row['list_of_parents_from_fdic']=x
                                row['parent_city_distance'] = p_c_d
                                row['list_of_parents_with same city state']=y
                                row['final_title'] = i
                                row['final active Name'] = matc.loc[index[0][0],'NAME']
                                row['final active city'] = matc.loc[index[0][0],'CITY']
                                row['final active state'] = matc.loc[index[0][0],'STATE']
                                variable_f = dnb_proccess(row,'final active Name','data from FDIC Events')
                                list_of_vari=list_of_vari.append(variable_f)

                                

                if co>0:                  

                    row=list_of_vari
                    row['list_of_parents_from_fdic']=x
                    row['co']=co
                    if co==1:
                        row['FDIC_INACTIVE_PARENT_indicator']=1
                    else:
                        row['TEMP2']='Y'
                        row['FDIC_INACTIVE_PARENT_indicator']=0
                    return row
                else:
                    row['Comments']='For fdic inactive list, none of there locations is matching'
                    matc=fdic[fdic.NAME==active_name]
                    matc.reset_index(inplace=True, drop=True)
                    if len(matc.CERT.unique())==1:
                        t=matc[matc.STATE==state]
                        if kre==0:
                            if len(t)>0:
                                if len(matc[matc['MAINOFF'] == 1]) ==1:          
                                    row['HQ'] = list(matc[matc['MAINOFF'] == 1]['CITY'])[0]

                                row['active_name'] = active_name
                                row['detailed_info']='Fdic name and state matching and unique ID'
                                row['final active Name'] = active_name
                                row = dnb_proccess(row,'active_name','Fdic name and state matching and unique ID')
                                return row
                    x=x.split(',',1)[1]
                    row['list_of_parents_from_fdic']=x                    
                    row['co']=co
                    a=row['SOURCE']
                    if "LQ" == a[-2:]:
                        row['dnb_name'] = 'closed entity'
                        row['detailed_info']= 'BANK Liquidates'
                        row['detailed_info']= 'BANK Liquidates'
                        row['dun_num']=-1
                        row['match_ind']=''              
                        row['gdun_final'] = -21000
                        row['DUNS_count']=-1
                        row['Comments']=''
                        row['gdun_name'] = 'closed entity'
                        return row
                    else:
                        temp_50=0
                        for i in list_1:
                            row['parent'] = i
                            matc21=title_change[title_change.NAME==change_name]
                            matc22=matc21[matc21.PARENT_NAME==i]
                            if len(matc22)==0:
                                continue
                            matc22.reset_index(inplace = True,drop = True)
                            matc22_vector,matc22_knn = create_KNN(matc22['CITY'])
                            vec = matc22_vector.transform([city])
                            city_dist,city_ind = matc22_knn.kneighbors(vec)
                            e_city=matc22.loc[city_ind[0][0],'CITY']
                            if city_dist<=0.2:
                                matc23=matc22[matc22.CITY==e_city]
                                if len(matc23)==0:
                                    continue
                                matc23.reset_index(inplace=True, drop=True)
                                matc23_vector,matc23_knn = create_KNN(matc23['STATE'])
                                vec = matc23_vector.transform([state])
                                state_dist,state_ind = matc23_knn.kneighbors(vec)
                                state_match = matc23.loc[state_ind[0][0],'STATE']
                                matc24=matc23[matc23.STATE==state_match]
                                if len(matc24)==1:
                                    matc24.reset_index(inplace=True, drop=True)
                                    row['list_of_parents_from_fdic']=x
                                    row['list_of_parents_with same city state']=y
                                    row['final_title'] = i
                                    row['final active Name'] = i
                                    row['SOURCE'] = matc23.loc[state_ind[0][0],'SOURCE']
                                    row['HQ'] = matc24.loc[state_ind[0][0],'PARENT_CITY']
                                    temp_50=temp_50+1
                        if temp_50==1:
                            
                            row = dnb_proccess(row,'final active Name','data from FDIC Events')
                            return row
                        else:              
                            row['dnb_name'] = 'manual'
                            row['final active Name'] = ""
                            row['detailed_info']= 'Bank with similar name in active and inactive but locations of either is not matching'
                            row['Comments']= 'Bank with similar name in active and inactive but locations of either is not matching'
                            row['gdun_final'] = -1
                            row['dun_num']=-1
                            row['SOURCE']=''
                            row['DUNS_count']=-1
                            row['match_ind']=''
                            row['gdun_name'] = 'Manual'
                            return row
            matc=fdic[fdic.NAME==active_name]
            matc.reset_index(inplace=True, drop=True)
            if len(matc.CERT.unique())==1:
                t=matc[matc.STATE==state]
                if len(t)>0:
                    if len(matc[matc['MAINOFF'] == 1]) ==1:          
                        row['HQ'] = list(matc[matc['MAINOFF'] == 1]['CITY'])[0]

                    row['active_name'] = active_name
                    row['detailed_info']='Fdic name and state matching and unique ID'
                    row['final active Name'] = active_name
                    row = dnb_proccess(row,'active_name','Fdic name and state matching and unique ID')
                    return row
                else:
                    row = credit_proccess(row,'NAME')
                    return row 
            else:
                row = credit_proccess(row,'NAME')
                return row    
            
    else:
        vec = title_change_vector.transform([name])
        distance,index = title_change_knn.kneighbors(vec)
        row['title_distance'] = distance[0][0]
        change_name=title_change.loc[index[0][0],'NAME']
        if distance[0][0] <= 0.2:
            matc22=title_change[title_change.NAME==change_name]
            matc22.reset_index(inplace = True,drop = True)
            list_1=matc22.PARENT_NAME.unique()
            if len(list_1)==0:
                row = dnb_proccess(row,'NAME','')
                return row
            if len(list_1)==1:
                row['parent_unique_count']=1
            else:
                row['parent_unique_count']=0
            if len(list_1)==1:                        
                vec = fdic_vector.transform([list_1[0]])
                distance,index = fdic_knn.kneighbors(vec)
                some_match = fdic.loc[index[0][0],'NAME']
                if distance[0][0] <= 0.2:
                    temp = fdic[fdic.NAME == some_match]
                    temp.reset_index(inplace = True,drop = True)
                    if len(temp[temp['MAINOFF'] == 1]) ==1:                        
                        row['HQ'] = list(temp[temp['MAINOFF'] == 1]['CITY'])[0]
                    row['SOURCE']=matc22['SOURCE'][0]
                    a=row['SOURCE']
                    if "LQ" == a[-2:]:
                        row['dnb_name'] = 'closed entity'
                        row['detailed_info']= 'BANK Liquidates'
                        row['gdun_final'] = -21000
                        row['dun_num']=-1
                        row['match_ind']='' 
                        row['DUNS_count']=-1
                        row['Comments']=''
                        row['gdun_name'] = 'closed entity'
                        return row
                    row['final_title'] = list_1[0]
                    row['final active Name'] = list_1[0]
                    row['list_of_parents_from_fdic']=list_1[0]
                    row = dnb_proccess(row,'final_title','data from FDIC Events')
                    return row  
                o_name = str(row['Orginal_Name']).lower()
                print(o_name)
                if o_name[-3:]==' cu' or o_name[-4:]==' fcu' or o_name[-3:]==',cu' or o_name[-4:]==',fcu' or  o_name[-6:]==' union' in o_name:
                    vec = active_vector.transform([list_1[0]])
                    distance,index = active_knn.kneighbors(vec)
                    some_match = active.loc[index[0][0],'NAME']  
                    if distance[0][0] <= 0.2:
                        matc=active[active.NAME==some_match]
                        matc.reset_index(inplace=True, drop=True)
                        matc_vector,matc_knn = create_KNN(matc['STATE'])
                        vec = matc_vector.transform([state])
                        state_dist,state_ind = matc_knn.kneighbors(vec)
                        state_match = matc.loc[state_ind[0][0],'STATE']
                        temp=matc[matc.STATE==state]
                        temp.reset_index(inplace=True, drop=True)
                        if len(temp.ID_NCUA.unique())==1:
                            row['Credit_final_parent'] = matc.loc[state_ind[0][0],'NAME']
                            row['SOURCE']='CREDITUNION_ACTIVE'
                            row['active_name'] = matc.loc[state_ind[0][0],'NAME']
                            row['active_credit_name'] = matc.loc[state_ind[0][0],'NAME']
                            row['detailed_info']= 'inactive parent mapped in FDIC to a active credit union'
                            row=dnb_c_proccess(row,'active_credit_name','inactive parent mapped in FDIC to a active credit union')
                            return row
                        else:
                            row['SOURCE']='CREDITUNION_ACTIVE'
                            row['Credit_final_parent'] = matc.loc[state_ind[0][0],'NAME']
                            row['active_credit_name'] = matc.loc[state_ind[0][0],'NAME']
                            row['detailed_info']= 'inactive parent mapped in FDIC to a active credit union'
                            row['active_name'] = matc.loc[state_ind[0][0],'NAME']

                            row=dnb_c_proccess(row,'active_credit_name','inactive parent mapped in FDIC to a active credit union')
                            return row
                    else:
                        row=dnb_proccess(row,'NAME',"")
                        return row
                else:                  
                    row = credit_proccess(row,'NAME')
                    return row
            x=''
            y=''
            co=0
            g=''
            yu=''
            kre=0
            list_of_vari=pd.DataFrame()
            for i in list_1:
                row['parent'] = str(i)
                x=x+","+i
                vec = fdic_vector.transform([row['parent']])
                distance,index = fdic_knn.kneighbors(vec)
                p_ac_dis = distance[0][0]
                some_match = fdic.loc[index[0][0],'NAME']
                row['SOURCE']=matc22.loc[matc22.PARENT_NAME==i,'SOURCE'].values[0]
                if distance[0][0] <= 0.2:
                    temp = fdic[fdic.NAME == some_match]
                    temp.reset_index(inplace = True,drop = True)
                    if len(temp) == 0:
                        pass
                    
                    temp_vector,temp_knn = create_KNN(temp['STATE'])
                    vec = temp_vector.transform([state])
                    distance,index = temp_knn.kneighbors(vec)
                    p_c_d = distance[0][0]
                    state_match = temp.loc[index[0][0],'STATE']
                    if distance==0:
                        matc=temp[temp.STATE==state_match]
                        matc.reset_index(inplace=True, drop=True)
                        if len(matc) == 0:
                            pass
                        kre=kre+1
                        matc_vector,matc_knn = create_KNN(matc['CITY'])
                        vec = matc_vector.transform([city])
                        distance,index = matc_knn.kneighbors(vec)
                        row['ignore_p'] =i
                        row['pa']=p_ac_dis
                        city_match = matc.loc[index[0][0],'CITY']
                        if distance[0][0] <= 0.2:
                            y=y+","+i
                            co=co+1
                            matc34=matc[matc.CITY==city_match]
                            matc34.reset_index(inplace=True, drop=True)
                            a0=matc34.CERT.unique()
                            a1=a0[0]
                            a2=temp[temp['CERT'] ==a1 ]
                            if len(a2[a2['MAINOFF'] == 1]) ==1:          
                                row['HQ'] = list(a2[a2['MAINOFF'] == 1]['CITY'])[0]
                            row['Parent_active_distance'] = p_ac_dis
                            row['list_of_parents_from_fdic']=x
                            row['parent_city_distance'] = p_c_d
                            row['list_of_parents_with same city state']=y
                            row['final_title'] = i
                            row['final active Name'] = matc.loc[index[0][0],'NAME']
                            row['final active city'] = matc.loc[index[0][0],'CITY']
                            row['final active state'] = matc.loc[index[0][0],'STATE']
                            variable_f = dnb_proccess(row,'final active Name','data from FDIC Events')
                            list_of_vari=list_of_vari.append(variable_f)

            if co>0:
                row=list_of_vari
                row['list_of_parents_from_fdic']=x
                row['co']=co
                if co==1:
                    row['FDIC_INACTIVE_PARENT_indicator']=0
                else:
                    row['TEMP2']='Y'
                    row['FDIC_INACTIVE_PARENT_indicator']=1
                return row
            
            else:
                row['SOURCE']=""
                x=x.split(',',1)[1]
                row['list_of_parents_from_fdic']=x
                row['co']=co
                a=row['SOURCE']
                if "LQ" == a[-2:]:
                    row['dnb_name'] = 'closed entity'
                    row['detailed_info']= 'BANK Liquidates'
                    row['gdun_final'] = -21000
                    row['dun_num']=-1
                    row['match_ind']=''  
                    row['gdun_name'] = 'closed entity'
                    row['DUNS_count']=-1
                    row['Comments']=''
                    return row
                else:
                    temp_50=0
                    for i in list_1:
                        row['parent'] = i
                        matc21=title_change[title_change.NAME==change_name]
                        matc22=matc21[matc21.PARENT_NAME==i]
                        matc22.reset_index(inplace = True,drop = True)
                        if len(matc22)==0:
                            continue
                        matc22_vector,matc22_knn = create_KNN(matc22['CITY'])
                        vec = matc22_vector.transform([city])
                        city_dist,city_ind = matc22_knn.kneighbors(vec)
                        e_city=matc22.loc[city_ind[0][0],'CITY']
                        if city_dist<=0.2:
                            matc23=matc22[matc22.CITY==e_city]
                            if len(matc23)==0:
                                continue
                            print(matc23)
                            matc23.reset_index(inplace=True, drop=True)
                            matc23_vector,matc23_knn = create_KNN(matc23['STATE'])
                            vec = matc23_vector.transform([state])
                            state_dist,state_ind = matc23_knn.kneighbors(vec)
                            state_match = matc23.loc[state_ind[0][0],'STATE']
                            matc24=matc23[matc23.STATE==state_match]
                            if len(matc24)==1:
                                matc24.reset_index(inplace=True, drop=True)
                                row['list_of_parents_from_fdic']=x
                                row['list_of_parents_with same city state']=y
                                row['final_title'] = i
                                row['final active Name'] = i
                                row['SOURCE'] = matc24.loc[state_ind[0][0],'SOURCE']
                                row['HQ'] = matc24.loc[state_ind[0][0],'PARENT_CITY']
                                temp_50=temp_50+1
                    if temp_50==1:
                            
                        row= dnb_proccess(row,'final active Name','data from FDIC Events')
                        return row
                    else:           
                        row = credit_proccess(row,'NAME')
                        return row
        else:
            row = credit_proccess(row,'NAME')
            return row    
    return row


def spnv_check(row):
    name = row['NAME']
    city = row['CITY']
    vec = cs_vector.transform([row['CITY']])
    dist,ind = cs_knn.kneighbors(vec)
    if dist[0][0]<=0.2:
        if city_st.loc[ind[0][0],'Count']==1:
            row['STATE']=city_st.loc[ind[0][0],'STATE']
    row=active_proccess(row,'NAME')
    return row


def spnv_check1(row):
    name = row['NAME']
    city = row['CITY']
    vec = cs_vector.transform([row['CITY']])
    dist,ind = cs_knn.kneighbors(vec)
    if dist[0][0]<=0.2:
        if city_st.loc[ind[0][0],'Count']==1:
            row['STATE']=city_st.loc[ind[0][0],'STATE']
    row=dnb_proccess(row,'NAME', 'non banking/non credit union entities')
    return row



for i in tqdm(range(len(current_run))):
    df = pd.DataFrame()
    f_row = current_run.loc[i,:]
    f_row = spnv_check(f_row)
    f_row['TEMP1']=i
    df = df.append(f_row)
    df.rename(columns = {'dun_num':'FJ_DUNS',
                     'Credit_final_parent':'CREDIT_UNION_PARENT',
                     'Orginal_Name':'SEC_PARTY_CMN_NM',
                     'Orginal_CITY':'SEC_PARTY_ADDR_CITY_NM',
                     'Orginal_STATE':'SEC_PARTY_ADDR_STATE_CD',
                     'NAME':'COMPRESSES_SEC_PARTY_NAME',
                     'CITY':'COMPRESSES_SEC_PARTY_CITY', 
                     'STATE':'COMPRESSES_SEC_PARTY_STATE',
                     'FILINGS':'FLININGS_COUNT',
                     'final active Name':'FINAL_FDIC_PARENT',
                     'gdun_final':'FJ_GDUN',
                     'gdun_name':'FJ_GDUN_NAME',
                     'dnb_name':'FJ_DUN_NAME',
                     'SECURED_PARTY_DUNS':'WK_DUNS',
                     'PERM_ID':'PERM_ID',
                     'match_ind':'MATCH_IND',
                     'OLD_GDUN':'WK_GDUN',
                     'OLD_GDUN_NAM':'WK_GDUN_NAME',
                     'dnb_name':'FJ_DUN_NAME',
                     'DUNS_count':'DUNS_COUNT',
                     'detailed_info':'DETAIL_INFO',
                     'Comments':'COMMENTS',
                     'active_name':'FDIC_ACTIVE_PARENT',
                     'list_of_parents_from_fdic':'FDIC_INACTIVE_PARENT_IDN',
                     'final_title':'FDIC_INACTIVE_PARENT_MAP',
                     'dnb_unique_Count':'DNB_UNIQUE_ID',
                     'active_dist':'ACTIVE_NAME_DISTANCE',
                     'active_city_dist':'ACTIVE_CITY_DISTANCE',
                     'title_distance':'INACTIVE_TITLE_DISTANCE',
                     'Parent_active_distance':'PARENT_NAME_DISTANCE',
                     'parent_city_distance': 'PARENT_CITY_DISTANCE',
                     'dnb_dist':'DNB_NAME_DISTANCE',
                     'active_unique_count':'ACTIVE_UNIQUE_ID',
                     'dnb_unique_Count':'DNB_UNIQUE_ID',
                     'list_of_duns':'LIST_OF_DUNS',
                     'parent_unique_count':'PARENT_UNIQUE_ID',
                     'dnb_city_distance':'DNB_CITY_DISTANCE',
                     'active_credit_dist':'CREDIT_UNION_ACTIVE_DIST'   ,        
                     'closed_dist':'CREDIT_UNION_CLOSED_DIST' ,    
                     'title_mna_dist':'CREDIT_UNION_MNA_DIST'  }, inplace = True)
    temp=df.reindex(columns=['PERM_ID','SEC_PARTY_CMN_NM','SEC_PARTY_COMPRESS_CMN_NM','SEC_PARTY_ADDR_CITY_NM','SEC_PARTY_ADDR_STATE_CD','FLININGS_COUNT','WK_DUNS','WK_GDUN_NAME' , 'WK_GDUN','COMPRESSES_SEC_PARTY_NAME','COMPRESSES_SEC_PARTY_CITY','COMPRESSES_SEC_PARTY_STATE', 'FDIC_ACTIVE_PARENT','FDIC_INACTIVE_PARENT_IDN','FDIC_INACTIVE_PARENT_MAP','FINAL_FDIC_PARENT', 'FJ_GDUN','FJ_GDUN_NAME','MATCH_IND',  'FJ_DUN_NAME',  'FJ_DUNS',   'DUNS_COUNT',      'SOURCE',      'DNB_UNIQUE_ID','ACTIVE_UNIQUE_ID',      'PARENT_UNIQUE_ID',     'ACTIVE_NAME_DISTANCE',   'ACTIVE_CITY_DISTANCE',  'INACTIVE_TITLE_DISTANCE', 'PARENT_NAME_DISTANCE',  'PARENT_CITY_DISTANCE',    'DNB_NAME_DISTANCE',         'DNB_CITY_DISTANCE',  'DETAIL_INFO',          'COMMENTS',            'CREDIT_UNION_PARENT','CREDIT_UNION_ACTIVE_DIST',   'CREDIT_UNION_CLOSED_DIST',      'CREDIT_UNION_MNA_DIST',       'TEMP1' ,         'TEMP2', 'LIST_OF_DUNS' ])
    
    temp.fillna('null',inplace=True)
    temp.reset_index(inplace=True, drop=True)
    j=0
    while j<len(temp):  
        valu=tuple(temp.iloc[j])
        valu = tuple([val.replace('"',"'") if isinstance(val, str) else val for val in valu])
        valu = tuple([val.replace("'","''") if isinstance(val, str) else val for val in valu])
        valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])
        valu=str(valu)
        valu=valu.replace("'null'","NULL")
        valu=valu.replace('"',"'")
        insert_query = f"""INSERT INTO MSTRSTG.SPNV_MATCH_RAW (PERM_ID, SEC_PARTY_CMN_NM, SEC_PARTY_COMPRESS_CMN_NM, SEC_PARTY_ADDR_CITY_NM, SEC_PARTY_ADDR_STATE_CD, FLININGS_COUNT, WK_DUNS, WK_GDUN_NAME, WK_GDUN, COMPRESSES_SEC_PARTY_NAME, COMPRESSES_SEC_PARTY_CITY, COMPRESSES_SEC_PARTY_STATE, FDIC_ACTIVE_PARENT, FDIC_INACTIVE_PARENT_IDN, FDIC_INACTIVE_PARENT_MAP, FINAL_FDIC_PARENT, FJ_GDUN, FJ_GDUN_NAME, MATCH_IND, FJ_DUN_NAME, FJ_DUNS, DUNS_COUNT, SOURCE, DNB_UNIQUE_ID, ACTIVE_UNIQUE_ID, PARENT_UNIQUE_ID, ACTIVE_NAME_DISTANCE, ACTIVE_CITY_DISTANCE, INACTIVE_TITLE_DISTANCE, PARENT_NAME_DISTANCE, PARENT_CITY_DISTANCE, DNB_NAME_DISTANCE, DNB_CITY_DISTANCE, DETAIL_INFO, COMMENTS, CREDIT_UNION_PARENT, CREDIT_UNION_ACTIVE_DIST, CREDIT_UNION_CLOSED_DIST, CREDIT_UNION_MNA_DIST, TEMP1, TEMP2, LIST_OF_DUNS)
VALUES {valu}"""
        try:
            cursor.execute(insert_query)

            conn.commit()
            j=j+1
        except:
            os.environ['ORACLE_HOME'] = connection_data['oracle_client']
            os.environ['LD_LIBRARY_PATH'] = connection_data['ld_library']
            with open('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Script/stag_credentials.json') as f:
                connection_data = json.load(f)

            dsn = cx_Oracle.makedsn(connection_data['host'], connection_data['port'], service_name=connection_data['SID'])
            conn = cx_Oracle.connect(user=connection_data['user'], password=connection_data['password'], dsn=dsn)
            cursor=conn.cursor()

            cursor.execute(insert_query)

            conn.commit()
            j=j+1

for i in tqdm(range(len(current_run1))):
    df = pd.DataFrame()
    f_row = current_run1.loc[i,:]
    f_row = spnv_check1(f_row)
    f_row['TEMP1']=i
    df = df.append(f_row)
    df.rename(columns = {'dun_num':'FJ_DUNS',
                     'Credit_final_parent':'CREDIT_UNION_PARENT',
                     'Orginal_Name':'SEC_PARTY_CMN_NM',
                     'Orginal_CITY':'SEC_PARTY_ADDR_CITY_NM',
                     'Orginal_STATE':'SEC_PARTY_ADDR_STATE_CD',
                     'NAME':'COMPRESSES_SEC_PARTY_NAME',
                     'CITY':'COMPRESSES_SEC_PARTY_CITY', 
                     'STATE':'COMPRESSES_SEC_PARTY_STATE',
                     'FILINGS':'FLININGS_COUNT',
                     'final active Name':'FINAL_FDIC_PARENT',
                     'gdun_final':'FJ_GDUN',
                     'gdun_name':'FJ_GDUN_NAME',
                     'dnb_name':'FJ_DUN_NAME',
                     'SECURED_PARTY_DUNS':'WK_DUNS',
                     'PERM_ID':'PERM_ID',
                     'match_ind':'MATCH_IND',
                     'OLD_GDUN':'WK_GDUN',
                     'OLD_GDUN_NAM':'WK_GDUN_NAME',
                     'dnb_name':'FJ_DUN_NAME',
                     'DUNS_count':'DUNS_COUNT',
                     'detailed_info':'DETAIL_INFO',
                     'Comments':'COMMENTS',
                     'active_name':'FDIC_ACTIVE_PARENT',
                     'list_of_parents_from_fdic':'FDIC_INACTIVE_PARENT_IDN',
                     'final_title':'FDIC_INACTIVE_PARENT_MAP',
                     'dnb_unique_Count':'DNB_UNIQUE_ID',
                     'active_dist':'ACTIVE_NAME_DISTANCE',
                     'active_city_dist':'ACTIVE_CITY_DISTANCE',
                     'title_distance':'INACTIVE_TITLE_DISTANCE',
                     'Parent_active_distance':'PARENT_NAME_DISTANCE',
                     'parent_city_distance': 'PARENT_CITY_DISTANCE',
                     'dnb_dist':'DNB_NAME_DISTANCE',
                     'active_unique_count':'ACTIVE_UNIQUE_ID',
                     'dnb_unique_Count':'DNB_UNIQUE_ID',
                     'list_of_duns':'LIST_OF_DUNS',
                     'parent_unique_count':'PARENT_UNIQUE_ID',
                     'dnb_city_distance':'DNB_CITY_DISTANCE',
                     'active_credit_dist':'CREDIT_UNION_ACTIVE_DIST'   ,        
                     'closed_dist':'CREDIT_UNION_CLOSED_DIST' ,    
                     'title_mna_dist':'CREDIT_UNION_MNA_DIST'  }, inplace = True)
    temp=df.reindex(columns=['PERM_ID','SEC_PARTY_CMN_NM','SEC_PARTY_COMPRESS_CMN_NM','SEC_PARTY_ADDR_CITY_NM','SEC_PARTY_ADDR_STATE_CD','FLININGS_COUNT','WK_DUNS','WK_GDUN_NAME' , 'WK_GDUN','COMPRESSES_SEC_PARTY_NAME','COMPRESSES_SEC_PARTY_CITY','COMPRESSES_SEC_PARTY_STATE', 'FDIC_ACTIVE_PARENT','FDIC_INACTIVE_PARENT_IDN','FDIC_INACTIVE_PARENT_MAP','FINAL_FDIC_PARENT', 'FJ_GDUN','FJ_GDUN_NAME','MATCH_IND',  'FJ_DUN_NAME',  'FJ_DUNS',   'DUNS_COUNT',      'SOURCE',      'DNB_UNIQUE_ID','ACTIVE_UNIQUE_ID',      'PARENT_UNIQUE_ID',     'ACTIVE_NAME_DISTANCE',   'ACTIVE_CITY_DISTANCE',  'INACTIVE_TITLE_DISTANCE', 'PARENT_NAME_DISTANCE',  'PARENT_CITY_DISTANCE',    'DNB_NAME_DISTANCE',         'DNB_CITY_DISTANCE',  'DETAIL_INFO',          'COMMENTS',            'CREDIT_UNION_PARENT','CREDIT_UNION_ACTIVE_DIST',   'CREDIT_UNION_CLOSED_DIST',      'CREDIT_UNION_MNA_DIST',       'TEMP1' ,         'TEMP2', 'LIST_OF_DUNS' ])
    
    temp.fillna('null',inplace=True)
    temp.reset_index(inplace=True, drop=True)
    j=0
    while j<len(temp):  
        valu=tuple(temp.iloc[j])
        valu = tuple([val.replace('"',"'") if isinstance(val, str) else val for val in valu])
        valu = tuple([val.replace("'","''") if isinstance(val, str) else val for val in valu])
        valu = tuple([int(val) if isinstance(val, np.int64) else val for val in valu])
        valu=str(valu)
        valu=valu.replace("'null'","NULL")
        valu=valu.replace('"',"'")
        insert_query = f"""INSERT INTO MSTRSTG.SPNV_MATCH_RAW (PERM_ID, SEC_PARTY_CMN_NM, SEC_PARTY_COMPRESS_CMN_NM, SEC_PARTY_ADDR_CITY_NM, SEC_PARTY_ADDR_STATE_CD, FLININGS_COUNT, WK_DUNS, WK_GDUN_NAME, WK_GDUN, COMPRESSES_SEC_PARTY_NAME, COMPRESSES_SEC_PARTY_CITY, COMPRESSES_SEC_PARTY_STATE, FDIC_ACTIVE_PARENT, FDIC_INACTIVE_PARENT_IDN, FDIC_INACTIVE_PARENT_MAP, FINAL_FDIC_PARENT, FJ_GDUN, FJ_GDUN_NAME, MATCH_IND, FJ_DUN_NAME, FJ_DUNS, DUNS_COUNT, SOURCE, DNB_UNIQUE_ID, ACTIVE_UNIQUE_ID, PARENT_UNIQUE_ID, ACTIVE_NAME_DISTANCE, ACTIVE_CITY_DISTANCE, INACTIVE_TITLE_DISTANCE, PARENT_NAME_DISTANCE, PARENT_CITY_DISTANCE, DNB_NAME_DISTANCE, DNB_CITY_DISTANCE, DETAIL_INFO, COMMENTS, CREDIT_UNION_PARENT, CREDIT_UNION_ACTIVE_DIST, CREDIT_UNION_CLOSED_DIST, CREDIT_UNION_MNA_DIST, TEMP1, TEMP2, LIST_OF_DUNS)
VALUES {valu}"""
        try:
            cursor.execute(insert_query)

            conn.commit()
            j=j+1
        except:
            os.environ['ORACLE_HOME'] = connection_data['oracle_client']
            os.environ['LD_LIBRARY_PATH'] = connection_data['ld_library']

            with open('/application/INFA/server/infa_shared/Scripts/FJ_GDUN_Automation/Script/stag_credentials.json') as f:
                connection_data = json.load(f)

            dsn = cx_Oracle.makedsn(connection_data['host'], connection_data['port'], service_name=connection_data['SID'])
            conn = cx_Oracle.connect(user=connection_data['user'], password=connection_data['password'], dsn=dsn)
            cursor=conn.cursor()

            cursor.execute(insert_query)

            conn.commit()
            j=j+1




cursor.close()
conn.close()