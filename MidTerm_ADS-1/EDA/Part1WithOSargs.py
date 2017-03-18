
# coding: utf-8

# In[1]:

import sys


# In[2]:

import os


# In[21]:

import pandas as pd


# In[16]:

import pylab as pl


# In[18]:

import matplotlib.pyplot as plt


# In[19]:

import numpy as np


# In[20]:

from zipfile import ZipFile


# In[21]:

import zipfile


# In[22]:

from io import BytesIO


# In[23]:

import scipy.stats as stats


# In[24]:

from urllib.request import urlretrieve


# In[25]:

from urllib.request import urlopen


# In[14]:

from bs4 import BeautifulSoup


# In[2]:

import requests


# In[3]:

from requests import session


# In[23]:




# In[4]:

username = sys.argv[1]


# In[5]:

password = sys.argv[2]


# In[4]:

session = requests.session()


# In[5]:

login_url = 'https://freddiemac.embs.com/FLoan/secure/auth.php'


# In[6]:

values = {'username': username,'password': password}


# In[7]:

r = session.post(login_url,data=values)


# In[8]:

r.status_code


# In[9]:

valuesNew={'accept':'Yes', 'acceptSubmit':'Continue', 'action':'acceptTandC'}


# In[10]:

url='https://freddiemac.embs.com/FLoan/Data/download.php'


# In[11]:

login=session.post(url,data=valuesNew)


# In[12]:

page=login.content


# In[26]:

soup = BeautifulSoup(page)


# In[27]:

links = soup.find_all('a')


# In[28]:
import os
import seaborn as sns
from datetime import datetime
dir='C:/Users/Vaidehi Deshpande/Documents/vaidehi/NEU/Subjects'
os.chdir(dir)

# In[39]:

def ReplaceMissingData(df):
    #df.Col1.apply(lambda x: x.strip()).replace('', np.nan)
    df.Col1.fillna(df.Col1.mean(), inplace =True)
    df.Col2.fillna(0,inplace=True )
    df.Col3.fillna('unknown', inplace =True)
    df.Col4.fillna(0, inplace=True)
    df.Col5.fillna(df.Col5.median(),inplace = True)
    df.Col6.fillna(df.Col6.mean(), inplace=True)
    df.Col7.fillna(df.Col7.mode(), inplace=True)
    df.Col8.fillna(df.Col8.mode(), inplace=True)
    df.Col9.fillna(df.Col9.mode(),inplace=True)
    df.Col10.fillna(df.Col10.mean(), inplace = True)
    df.Col11.fillna(df.Col11.mean(), inplace=True)
    df.Col12.fillna(df.Col12.mode(),inplace=True)
    df.Col13.fillna(df.Col13.mean(), inplace=True)
    df.Col14.fillna(df.Col14.mode(), inplace=True)
    df.Col15.fillna(df.Col15.mode(), inplace = True)
    df.Col16.fillna('U', inplace=True)
    df.Col17.fillna('U', inplace=True)
    df.Col18.fillna('U', inplace=True)
    df.Col19.fillna(df.Col19.mode(),inplace=True)
    df.Col21.fillna('U', inplace=True)
    df.Col22.fillna(0,inplace=True)
    df.Col23.fillna(df.Col23.mode(),inplace=True)
    df.Col24.fillna('unknown', inplace=True)
    df.Col25.fillna('unknown', inplace=True)
    df.Col26.fillna('N',inplace=True)
    
    return df


# In[30]:

import io


# In[38]:

def HandlePerfData(df):
    df.ZeroBalCode.fillna(10, inplace = True)
    return df

def HandlePerformanceData(df):
    df.Col7.fillna('NA', inplace = True)
    df.Col8.fillna('N', inplace = True)
    df.Col9.fillna(10, inplace = True)
    df.Col10.fillna(190012, inplace = True)
    df.Col13.fillna(190012, inplace = True)
    df.Col14.fillna(0.0, inplace = True)
    #df.Col15.fillna(-1.0, inplace = True)
    df.Col16.fillna(-1.0, inplace=True)
    df.Col18.fillna(0.0, inplace = True)
    df.Col19.fillna(0.0, inplace =True)
    df.Col20.fillna(0.0, inplace =True)
    df.Col21.fillna(0.0, inplace =True)
    df.Col22.fillna(0.0, inplace =True)
    return df

# In[33]:

def Quarter(month):
    
    if month < 4:
        return 1
    elif (month > 3 and month < 7):
        return 2
    elif (month > 6 and month < 10):
        return 3
    else:
        return 4
    #return 0

def summarizeOrigFile(df):
    QuarterCount=df.groupby('Quarter').agg({'Col20':'count'}).reset_index()
    QuarterCount.to_csv("SummarizedQuarterlyOrigFile.csv",mode='w', sep=',',index=False)

    total_upb = df.groupby('Quarter').agg({'Col11':'sum'}).reset_index()


    creditScore = df.groupby('Quarter').agg({'Col1': 'mean'}).reset_index()
    #creditScore.to_csv("SummarizedOrigFile.csv",mode='a', sep=',',index=False)

    FirstProperty= df[df.Col3 =='Y']
    FirstPropertyCount= FirstProperty.groupby('Quarter').agg({'Col3':'count'}).reset_index()
   # FirstPropertyCount.to_csv("SummarizedOrigFile.csv",mode='a', sep=',',index=False)

    NotFirstProperty=df[df.Col3=='N']
    NotFirstPropertyCount=NotFirstProperty.groupby('Quarter').agg({'Col3':'count'}).reset_index()

    interestRate=df.groupby('Quarter').agg({'Col13':'mean'}).reset_index()

    new_csv=pd.read_csv("SummarizedQuarterlyOrigFile.csv", sep=',')
    new_csv['Total UPB']=total_upb['Col11']
    new_csv['Credit Score']=creditScore['Col1']
    new_csv['CountFirstProperty']=FirstPropertyCount['Col3']
    new_csv['CountNotFirstProperty']=NotFirstPropertyCount['Col3']
    new_csv['Interest Rate']=interestRate['Col13']
    new_csv.to_csv("SummarizedQuarterlyOrigFile.csv",mode='w', sep=',',index=False)

    StateCount=df.groupby('Col17').agg({'Col20':'count'}).reset_index()
    TotalUPB=df.groupby('Col17').agg({'Col11':'sum'}).reset_index()
    CreditScr=df.groupby('Col17').agg({'Col1': 'mean'}).reset_index()
    IntRate=df.groupby('Col17').agg({'Col13':'mean'}).reset_index()
    StateCount['Total UPB']=TotalUPB['Col11']
    StateCount['Credit Score']=CreditScr['Col1']
    StateCount['Interest Rate']=IntRate['Col13']
    StateCount.to_csv("SummarizedStateOrigFile.csv", sep=',', index=False)
    #NotFirstPropertyCount.to_csv("SummarizedOrigFile.csv",mode='a', sep=',',index=False)

    countofPT = len(df.Col18)

    condo=df[df.Col18=='CO']
    condoCount=len(condo.Col18)
    condoPercent=condoCount/countofPT

    Leasehold=df[df.Col18=='LH']
    leaseHoldCount=len(Leasehold.Col18)
    leaseHoldPercent=leaseHoldCount/countofPT

    PUD=df[df.Col18=='PU']
    PUDCount=len(PUD.Col18)
    PUDPercent=PUDCount/countofPT

    ManfHousing=df[df.Col18=='MH']
    mhousingCount=len(ManfHousing.Col18)
    mHousingPercent=mhousingCount/countofPT

    FeeSimple=df[df.Col18=='SF']
    feeSimpleCount=len(FeeSimple.Col18)
    feeSimplePercent=feeSimpleCount/countofPT

    co_op=df[df.Col18=='CP']
    coopCount=len(co_op.Col18)
    coopPercent=coopCount/countofPT

    others=df[df.Col18=='U']
    othersCount=len(others.Col18)
    othersPercent=othersCount/countofPT

    dfPT=pd.DataFrame(columns={'Total','condo','leaseHold','PUD','ManufactureHousing', 'FeeSimple', 'Coop', 'Others'},index=[0])

    dfPT.Total.iloc[0] = 1
    dfPT.condo.iloc[0]=condoPercent
    dfPT.leaseHold.iloc[0]=leaseHoldPercent
    dfPT.PUD.iloc[0]=PUDPercent
    dfPT.ManufactureHousing.iloc[0]=mHousingPercent
    dfPT.FeeSimple.iloc[0]=feeSimplePercent
    dfPT.Coop.iloc[0]=coopPercent
    dfPT.Others.iloc[0]=othersPercent

    return dfPT.to_csv("SummarizedPercentOrigFile.csv",sep=',')




     
# In[34]:

def summarizePerfFile(df):
    frames=[]
    Nodelinq = df[df.Col4=='0']
    delinq =df[df.Col4!='0']
    totalDelCount=df.groupby('Quarter').agg({'Col4':'count'}).reset_index()
    delinqCount=delinq.groupby('Quarter').agg({'Col4':'count'}).reset_index()
    NodelinqCount=Nodelinq.groupby('Quarter').agg({'Col4':'count'}).reset_index()
    interestRate=df.groupby('Quarter').agg({'Col11':'mean'}).reset_index()
    dai=df.groupby('Quarter').agg({'DAI':'sum'}).reset_index()
    actualLoss=df.groupby('Quarter').agg({'Col22':'sum'}).reset_index()
    dfSumm=pd.DataFrame(columns={'Quarter','NotDelinquentCount', 'DelinquentCount', 'InterestRate', 'DAI','ActualLoss'})
    dfSumm.Quarter=NodelinqCount.Quarter
    dfSumm.NotDelinquentCount=NodelinqCount.Col4
    dfSumm.DelinquentCount=delinqCount.Col4
    dfSumm.InterestRate=interestRate.Col11
    dfSumm.DAI=dai.DAI
    dfSumm.ActualLoss=actualLoss.Col22

    return dfSumm.to_csv("SummarizedPerfFile.csv", sep=',')
    


if "Enter password:" in login.text:
    print("Please enter correct credentials or sign up at Freddie Mac website")
    
else:
    print("You are logged in")
    print("Enter 1 if you want to download whole dataset and 2 for specific year:")
    userIn = input()
    if "%s"%userIn=='1':
        appendWrite = True
        for link in links:
            if "sample" in link.text:
                #print("Found sample file")            
                response = urlopen("https://freddiemac.embs.com/FLoan/Data/"+link.text)
                if response.getcode() == 200:
                    data = response.read()
                    if zipfile.is_zipfile(BytesIO(data)) == True:
                        z=zipfile.ZipFile(BytesIO(data))
                        print(z.namelist())
                        print("Enter 1 for Origination dataset and 2 for Monthly Performance dataset")
                        selectIn=input()
                        if "%s"%selectIn=='1':
                            for file in z.namelist():
                                if "orig" in str(file):
                                    #print(str(file))
                                    if appendWrite:
                                        year = link.text[7:11]
                                        textFile = z.read(file)
                                        print("Reading zip files")
                                        df = pd.read_csv(BytesIO(textFile), sep = "|",header = None)
                                        df.columns = ['Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11', 'Col12','Col13', 'Col14', 'Col15', 'Col16', 'Col17', 'Col18', 'Col19', 'Col20', 'Col21', 'Col22', 'Col23', 'Col24', 'Col25', 'Col26']
                                        #year = df.Col2[1:4]
                                        df['year'] = year
                                    if not appendWrite:
                                        textFile = z.read(file)
                                        year = link.text[7:11]
                                        print("Reading zip file")
                                        #df = pd.read_csv(io.BytesIO(textFile),header=0)
                                        #df = handleMissingData(df.head(5))
                                        df3 = pd.read_csv(io.BytesIO(textFile),sep = "|",header=None)
                                        df3.columns = ['Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11', 'Col12','Col13', 'Col14', 'Col15', 'Col16', 'Col17', 'Col18', 'Col19', 'Col20', 'Col21', 'Col22', 'Col23', 'Col24', 'Col25', 'Col26']
                                        df3['year'] = year


                                        df = pd.concat([df,df3])
                                        #print("Files downloaded")
                                        df = ReplaceMissingData(df)
                                        df.to_csv("OriginationDataset.csv", sep=',',index=False)
                                        print("Origination dataset downloaded")
                                    appendWrite = False
                        if "%s"%selectIn=='2':
                            i=1999
                            frames=[]
                            while i<2017:
                                response = urlopen("https://freddiemac.embs.com/FLoan/Data/sample_"+str(i)+".zip")
                                if response.getcode() == 200:
                                    data = response.read()
                            #def ExtractFiles(data):
                                    if zipfile.is_zipfile(BytesIO(data)) == True:
                                        z=zipfile.ZipFile(BytesIO(data))
                                        for file in z.namelist():
                                            if "svcg" in str(file):
                                                print(str(file))
                                                textFile = z.read(file)
                                                print("Reading zip file")
                                                df2 = pd.read_csv(BytesIO(textFile), usecols=[0,3,8,10],names=['LoanSeqNum','DelinquencyStat', 'ZeroBalCode', 'CurrentRate'], sep = "|",header = None)
                                                #df.columns = ['Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11', 'Col12','Col13', 'Col14', 'Col15', 'Col16', 'Col17', 'Col18', 'Col19', 'Col20', 'Col21', 'Col22']
                                                df2['year'] = i
                                                df2=HandlePerfData(df2)
                                        
                                                frames.append(df2)
                                                print("dataframe"+str(i)+"appended")
                                                df=pd.DataFrame().append(frames)
                                                df.to_csv("PerformanceDataset.csv",sep=',',index=False)
                                                print("Performance dataset downloaded")
                        
                                    i=i+1

    if "%s"%userIn == '2':
        print("Enter the year (YYYY) for which you want to generate summary stats: ")
        yearIn = input()
        if "%s"%yearIn>'1998' and "%s"%yearIn<'2017':
            print("Enter 1 if you want to summarize Origination File and 2 if you want to summarize Performance file")
            fileIn = input()
            if "%s"%fileIn =='1':
                response = urlopen("https://freddiemac.embs.com/FLoan/Data/sample_"+"%s"%yearIn+".zip")
                if response.getcode() == 200:
                    data = response.read()
                    if zipfile.is_zipfile(BytesIO(data)) == True:
                        z=zipfile.ZipFile(BytesIO(data))
                        for file in z.namelist():
                            if "orig" in str(file):
                                textFile = z.read(file)
                                print("Reading Origination files")
                                df = pd.read_csv(BytesIO(textFile), sep = "|",header = None)
                                df.columns = ['Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11', 'Col12','Col13', 'Col14', 'Col15', 'Col16', 'Col17', 'Col18', 'Col19', 'Col20', 'Col21', 'Col22', 'Col23', 'Col24', 'Col25', 'Col26']
                                df.Col1 = pd.to_numeric(df.Col1, errors ='coerce')
                                df.Col6 = pd.to_numeric(df.Col6, errors ='coerce')
                                df.Col10 = pd.to_numeric(df.Col10, errors ='coerce')
                                print("Cleaning data")
                                df=ReplaceMissingData(df)
                                df.to_csv("OriginationFile %s"%yearIn+".csv",sep=',', encoding = 'utf-8')
                                month = df.Col2.astype(str).str[4:6].astype(int)
                                length = len(month)
                                i = 0
                                dataNew=[]
                                dataList=[]
                                while i < length:
                                    mon = month.iloc[i]
                                    dataNew = Quarter(mon)
                                    dataList.append(dataNew)
                                    i = i+1
                                df['Quarter'] = dataList
                                print("Processing summarizations")
                                summarizeOrigFile(df)
                    #df_summ.to_csv("OriginationSummaryStats %s"%yearIn+".csv",sep=',', encoding = 'utf-8')
            if "%s"%fileIn =='2':
                response = urlopen("https://freddiemac.embs.com/FLoan/Data/sample_"+"%s"%yearIn+".zip")
                if response.getcode() == 200:
                    data = response.read()
                    if zipfile.is_zipfile(BytesIO(data)) == True:
                        z=zipfile.ZipFile(BytesIO(data))
                        for file in z.namelist():
                            if "svcg" in str(file):
                                textFile = z.read(file)
                                print("Reading performance files")
                                df = pd.read_csv(BytesIO(textFile), sep = "|",header = None)
                                df.columns = ['Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11', 'Col12','Col13', 'Col14', 'Col15', 'Col16', 'Col17', 'Col18', 'Col19', 'Col20', 'Col21', 'Col22']
                                #df=HandlePerformanceData(df)
                                df.Col17 = df.Col18 + df.Col19 +df.Col20 +df.Col21
                                df.to_csv("PerformanceFile %s"%yearIn+".csv",sep=',', encoding = 'utf-8' )
                                print("Cleaning Data")
                                df=HandlePerformanceData(df)
                                month = df.Col2.astype(str).str[4:6].astype(int)
                                length = len(month)                          
                                dataNew=[]
                                dataList=[]
                                TempCols=[]
                                TempColsNew=[]
                                daiList=[]
                                m =[]
                                actualLoss=[]
                                i = 0
                                while i < length:
                                    mon = month.iloc[i]
                                    dataNew = Quarter(mon)
                                    dataList.append(dataNew)
                                    i=i+1
                                i=0
                                while i<len(df.Col2):
                                    a=str(int(df.Col2.iloc[i])) + "01"
                                    TempCols.append(a)
                                    i=i+1
                                i=0
                                while i<len(df.Col13):
                                    b=str(int(df.Col13.iloc[i])) + "01"
                                    TempColsNew.append(b)
                                    i=i+1
                                i=0
                                while i<len(TempCols):   
                                    x=datetime.strptime(TempCols[i], "%Y%m%d")
                                    y=datetime.strptime(TempColsNew[i], "%Y%m%d")
                                    z=(x.year - y.year)*12 + x.month - y.month
                                    m.append(z)
                                    i=i+1
                                i=0
                                while i<len(df):
                                    DAI=m[i]*df.Col3.iloc[i]*30/360*(df.Col11.iloc[i]-0.35)/100
                                    daiList.append(DAI)
                                    i=i+1
                                df['Quarter'] = dataList
                                df['DAI']=daiList
                                df.Col15=df.Col15.str.replace("U", "-20")
                                df.Col15=df.Col15.str.replace("C", "-10")
                                df.Col15=pd.to_numeric(df.Col15)
                                df.Col15.fillna(0, inplace=True)
                                i=0
                                while i<len(df):
                                    loss=(df.Col3.iloc[i] - df.Col15.iloc[i])-df.Col17.iloc[i] - df.Col14.iloc[i] - df.Col16.iloc[i]+ daiList[i]
                                    actualLoss.append(loss)
                                    
                                    i=i+1
                                df['Col22']=actualLoss
                                print("Generating summary statistics")
                                summarizePerfFile(df)
                #df_summ.to_csv("PerformanceSummaryStats %s"%yearIn+".csv",sep=',', encoding = 'utf-8')
                
            


# In[93]:




# In[36]:

#miss_percent=100*df.isnull().sum()/len(df)


# In[24]:

#df.shape


# In[ ]:

#df.Col1 = pd.to_numeric(df.Col1, errors ='coerce')


# In[ ]:

#df.Col6 = pd.to_numeric(df.Col6, errors ='coerce')


# In[42]:

#df.Col10 = pd.to_numeric(df.Col10, errors ='coerce')


# In[43]:

#df=ReplaceMissingData(df)


# In[44]:

#month = df.Col2.astype(str).str[4:6].astype(int)


# In[245]:

#%matplotlib inline
#sns.distplot(np.ravel(df.Col1))


# In[45]:

##length = len(month)


# In[46]:




# In[47]:




# In[48]:




# In[57]:




# In[101]:




# In[102]:

#count


# In[94]:

#total_upb = df.groupby(['year','Col17' ]).agg({'Col11':'sum'}).reset_index()


# In[95]:

#avg_upb = df.groupby(['year','Col17' ]).agg({'Col11':'mean'}).reset_index()


# In[96]:

#creditScore = df.groupby(['year','Col17' ]).agg({'Col1': 'mean'}).reset_index()


# In[97]:

#ltv = df.groupby(['year','Col17' ]).agg({'Col12': 'mean'}).reset_index()


# In[98]:

#cltv = df.groupby(['year','Col17' ]).agg({'Col9': 'mean'}).reset_index()


# In[99]:

#dti = df.groupby(['year','Col17' ]).agg({'Col10': 'mean'}).reset_index()


# In[100]:

#wac = df.groupby(['year','Col17' ]).apply(wavg, "Col13", "Col11")


# In[ ]:

#InterestRateVar = df.groupby('year').agg({'Col13':'var'}).reset_index()


# In[ ]:

#count_state = df.groupby('Col17').agg({'Col20':'count'}).reset_index()


# In[ ]:

#length = len(year_count['Col20'])


# In[103]:




# In[104]:

#df['OriginYear'] = df.Col2.astype(str).str[0:4]


# In[108]:




# In[133]:




# In[134]:




# In[136]:




# In[ ]:

#OriginYear = []
#OriginYear = df.Col2.astype(str).str[0:4]
    


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[127]:




# In[ ]:




# In[141]:

#df_summ.plot(x=['year','State'], y= 'wac' ,kind = 'line', figsize=(20,8), use_index = True, subplots=True)


# In[140]:

#plt.show()


# In[ ]:



