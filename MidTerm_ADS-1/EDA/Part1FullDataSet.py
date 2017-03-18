
# coding: utf-8

# In[72]:

import sys


# In[73]:

import os


# In[74]:

import pandas as pd


# In[75]:

import pylab as pl


# In[76]:

import matplotlib.pyplot as plt


# In[77]:

import numpy as np


# In[78]:

from zipfile import ZipFile


# In[79]:

import zipfile


# In[80]:

from io import BytesIO


# In[81]:

import scipy.stats as stats


# In[82]:

from urllib.request import urlretrieve


# In[83]:

from urllib.request import urlopen


# In[84]:

from bs4 import BeautifulSoup


# In[85]:

import requests


# In[86]:

from requests import session


# In[87]:

username = sys.argv[1]


# In[88]:

password = sys.argv[2]


# In[89]:

session = requests.session()


# In[90]:

login_url = 'https://freddiemac.embs.com/FLoan/secure/auth.php'


# In[91]:

values = {'username': 'deshpande.vai@husky.neu.edu','password': 'GYY^b~Dx'}


# In[92]:

r = session.post(login_url,data=values)


# In[93]:

r.status_code


# In[94]:

valuesNew={'accept':'Yes', 'acceptSubmit':'Continue', 'action':'acceptTandC'}


# In[95]:

url='https://freddiemac.embs.com/FLoan/Data/download.php'


# In[96]:

login=session.post(url,data=valuesNew)


# In[97]:

page=login.content


# In[98]:

soup = BeautifulSoup(page)


# In[99]:

links = soup.find_all('a')


# In[100]:

import seaborn as sns


# In[101]:

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


# In[135]:

def HandlePerformanceData(df):
   
    df.ZeroBalCode.fillna(10, inplace = True)
    return df


# In[102]:

def wavg(group, avg_name, weight_name):
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return 0


# In[103]:

import io


# In[104]:

appendWrite = True
for link in links:
    if "sample" in link.text:
        response = urlopen("https://freddiemac.embs.com/FLoan/Data/"+link.text)
        if response.getcode() == 200:
            data = response.read()
            if zipfile.is_zipfile(BytesIO(data)) == True:
                z=zipfile.ZipFile(BytesIO(data))
                #print(z.namelist())
                for file in z.namelist():
                    if "orig" in str(file):
                        #print(str(file))
                        if appendWrite:
                            year = link.text[7:11]
                            textFile = z.read(file)
                            df = pd.read_csv(BytesIO(textFile), sep = "|",header = None)
                            df.columns = ['Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11', 'Col12','Col13', 'Col14', 'Col15', 'Col16', 'Col17', 'Col18', 'Col19', 'Col20', 'Col21', 'Col22', 'Col23', 'Col24', 'Col25', 'Col26']
                            #year = df.Col2[1:4]
                            df['year'] = year
                        if not appendWrite:
                            textFile = z.read(file)
                            year = link.text[7:11]
                            df3 = pd.read_csv(io.BytesIO(textFile),sep = "|",header=None)
                            df3.columns = ['Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11', 'Col12','Col13', 'Col14', 'Col15', 'Col16', 'Col17', 'Col18', 'Col19', 'Col20', 'Col21', 'Col22', 'Col23', 'Col24', 'Col25', 'Col26']
                            df3['year'] = year
                            df = pd.concat([df,df3])
                        appendWrite = False


# In[106]:

df.Col1=pd.to_numeric(df.Col1, errors='coerce')


# In[107]:

df.Col6=pd.to_numeric(df.Col6, errors='coerce')


# In[108]:

df.Col10=pd.to_numeric(df.Col10, errors='coerce')


# In[110]:

df=ReplaceMissingData(df)


# In[124]:

count=df.groupby(['year']).agg({'Col20':'count'}).reset_index()


# In[111]:

total_upb = df.groupby(['year']).agg({'Col11':'sum'}).reset_index()


# In[112]:

avg_upb = df.groupby(['year' ]).agg({'Col11':'mean'}).reset_index()


# In[113]:

creditScore = df.groupby(['year' ]).agg({'Col1': 'mean'}).reset_index()


# In[114]:

ltv = df.groupby(['year' ]).agg({'Col12': 'mean'}).reset_index()


# In[115]:

cltv = df.groupby(['year' ]).agg({'Col9': 'mean'}).reset_index()


# In[116]:

dti = df.groupby(['year' ]).agg({'Col10': 'mean'}).reset_index()


# In[117]:

wac = df.groupby(['year' ]).apply(wavg, "Col13", "Col11")


# In[118]:

index = []
i = 0
while i < 18:
    index.append(i)
    i = i + 1


# In[119]:

columns =('year',  'count', 'Total Original UPB', 'Avg UPB', 'CreditScore', 'LTV', 'CLTV', 'dti', 'wac')


# In[120]:

df_summ = pd.DataFrame(index=index, columns = columns)


# In[125]:

i = 0
while i < 18:
    df_summ['year'][i] = count['year'][i]
    df_summ['count'][i] = count['Col20'][i]
    df_summ['Total Original UPB'][i] = total_upb['Col11'][i]
    df_summ['Avg UPB'][i] = avg_upb['Col11'][i]
    df_summ['CreditScore'][i] = creditScore['Col1'][i]
    df_summ['LTV'][i] = ltv['Col12'][i]
    df_summ['CLTV'][i] = cltv['Col9'][i]
    df_summ['dti'][i] = dti['Col10'][i]
    df_summ['wac'][i] = wac[i]
    #df_summ['InterestRateVariance'][i] = InterestRateVar['Col13'][i]
    #df_summ['InterestRate'][i] = InterestRate['Col13'][i]
    
    i = i + 1


# In[126]:

df_summ.to_csv("Part1OriginationSummaryStats.csv", sep = ',', encoding = 'utf-8')


# In[127]:

df_summ.plot(x='year', y= 'wac' ,kind = 'line', figsize=(20,8), use_index = True, subplots=True)


# In[128]:

plt.show()


# In[132]:

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
                    df2 = pd.read_csv(BytesIO(textFile), usecols=[0,3,8,10],names=['LoanSeqNum','DelinquencyStat', 'ZeroBalCode', 'CurrentRate'], sep = "|",header = None)
                    #df.columns = ['Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11', 'Col12','Col13', 'Col14', 'Col15', 'Col16', 'Col17', 'Col18', 'Col19', 'Col20', 'Col21', 'Col22']
                    df2['year'] = i
            
                    frames.append(df2)
                    print("dataframe"+str(i)+"appended")
                    
    i=i+1


# In[133]:

df_append= pd.DataFrame().append(frames)


# In[136]:

df_append=HandlePerformanceData(df_append)


# In[137]:

InterestRate=df_append.groupby('year').agg({'CurrentRate': 'mean'}).reset_index()


# In[138]:

Nodelinq = df_append[df_append.DelinquencyStat=='0']


# In[139]:

delinq= df_append[df_append.DelinquencyStat!='0']


# In[140]:

DelinqCount = delinq.groupby('year').agg({'DelinquencyStat':'count'}).reset_index()


# In[141]:

NotDelinqCount=Nodelinq.groupby('year').agg({'DelinquencyStat':'count'}).reset_index()


# In[142]:

totalDelinq=df_append.groupby('year').agg({'DelinquencyStat':'count'}).reset_index()


# In[143]:

NotDelinquentPercent=[]
i=0
while i<16:
    x = NotDelinqCount.DelinquencyStat.iloc[i]/totalDelinq.DelinquencyStat.iloc[i]
    NotDelinquentPercent.append(x)
    i=i+1
NotDelinquentPercent.append(0)
NotDelinquentPercent.append(0)


# In[144]:

DelinquentPercent=[]
i=0
while i<18:
    x = DelinqCount.DelinquencyStat.iloc[i]/totalDelinq.DelinquencyStat.iloc[i]
    DelinquentPercent.append(x)
    i=i+1


# In[145]:

year_count = df_append.groupby('year').agg({'LoanSeqNum':pd.Series.nunique}).reset_index()


# In[146]:

prepayCount = df_append[(df_append.ZeroBalCode == 06.0) | (df_append.ZeroBalCode == 01.0)]


# In[148]:

prepay=prepayCount.groupby('year').agg({'ZeroBalCode':'count'}).reset_index()


# In[149]:

prepayPercent=[]
i=0
while i<18:
    x = prepay.ZeroBalCode.iloc[i]/year_count.LoanSeqNum.iloc[i]
    prepayPercent.append(x)
    i=i+1


# In[152]:

cdeCount = df_append[(df_append.ZeroBalCode == 09.0) | (df_append.ZeroBalCode == 03.0)]


# In[153]:

cde = cdeCount.groupby('year').agg({'ZeroBalCode':'count'}).reset_index()


# In[154]:

cdePercent=[]
i=0
while i<17:
    x=cde.ZeroBalCode.iloc[i]/year_count.LoanSeqNum.iloc[i]
    cdePercent.append(x)
    i=i+1
cdePercent.append(0)


# In[155]:

i=0
remainingCount=[]
while i<17:
    x= (year_count.LoanSeqNum.iloc[i]-(prepay.ZeroBalCode.iloc[i]+cde.ZeroBalCode.iloc[i]))
    remainingCount.append(x)
    i=i+1
y=(year_count.LoanSeqNum.iloc[17]-prepay.ZeroBalCode.iloc[17])
remainingCount.append(y)


# In[156]:

i=0
remainingPercent=[]
while i<18:
    x=remainingCount[i]/year_count['LoanSeqNum'].iloc[i]
    remainingPercent.append(x)
    i=i+1


# In[157]:

index = []
i = 0
while i < 18:
    index.append(i)
    i = i + 1
columns=('Year','Count','PrePayPercent', 'cdePercent','RemainingPercent','CurrentInterestRate', 'DelinqPercent', 'NotDelinqPercent')


# In[158]:

df_summ = pd.DataFrame(index=index, columns = columns)


# In[159]:

i = 0
while i<18:
    df_summ['Year'].iloc[i] = year_count['year'].iloc[i]
    df_summ['Count'].iloc[i] = 50000
    df_summ['PrePayPercent'].iloc[i]=prepayPercent[i]
    df_summ['cdePercent'].iloc[i] = cdePercent[i]
    df_summ['RemainingPercent'].iloc[i] = remainingPercent[i]
    df_summ['DelinqPercent'].iloc[i]=DelinquentPercent[i]
    df_summ['NotDelinqPercent'].iloc[i]=NotDelinquentPercent[i]
    df_summ['CurrentInterestRate'].iloc[i]= InterestRate.CurrentRate.iloc[i]
    i=i+1
df_summ['cdePercent'].iloc[16]=0.00354
df_summ['Count'].iloc[17]=12500


# In[160]:

df_summ.to_csv("PerformanceSummarization.csv", sep = ',', encoding = 'utf-8')


# In[161]:

df_summ.plot(x='Year', y= 'DelinqPercent' ,kind = 'line', figsize=(20,8), use_index = True)


# In[162]:

plt.show()


# In[ ]:



