
# coding: utf-8

# In[1]:


import sys
import pandas as pd
import urllib.request
import zipfile, io
import csv
import os
import numpy as np
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import matplotlib
from scipy.stats import norm
import scipy.stats as stats
import matplotlib.mlab as mlab
import math
from urllib.request import urlopen
from urllib.request import urlretrieve
from requests import session
from lxml import html
import requests
import http.cookiejar
#import selenium
#from selenium import webdriver
#from selenium.webdriver.common.keys import Keys
#from bs4 import BeautifulSoup
import tabulate
import h2o
import seaborn as sns


# In[2]:

import sklearn
from sklearn import *
from sklearn.cross_validation import train_test_split
from sklearn.metrics import *
from IPython.display import HTML, display
import statsmodels.api as sm
from statsmodels.formula.api import ols
from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn import datasets, linear_model


# In[3]:

get_ipython().magic('matplotlib inline')
get_ipython().magic('matplotlib notebook')


# In[860]:

priorQuarterCleanData = 'priorQuarterCleanData.csv'
nextQuarterCleanData = 'nextQuarterCleanData.csv'
priorQuarter = 'priorQuarter.csv'
nextQuarter = 'nextQuarter.csv'
modelMetrics = 'modelMetrics.csv'
neuralNetworkMetrics = 'neuralNetworkMetrics.csv'


# In[5]:

quarter = os.getenv("Quarter")
year = os.getenv("Year")

#quarter = sys.argv[1]
#year = sys.argv[2]


# In[441]:

if(year is None or year==""):
    print("Please enter valid year")
    exit()
else:
    print("Year is:" + str(year))


# In[ ]:

#pathPriorquarter = ""
#pathNextquarter = ""


# In[6]:

# Validate input argument and set year to 2005 if validation fails
try:
    if len(year) >= 2:
        #if isinstance(year,int):
        year=int(year)
        print(year)
        if not(1999 <= year <=2016):
            print("Enter valid year")
    else:
        year=2005
except Exception as err:
    year=2005
print(year)


# In[7]:

try:
    if len(quarter) == 1:
        #if isinstance(year,int):
        quarter=int(quarter)
        print(quarter)
        if not(1 <= quarter <=4):
            print("Enter valid quarter")
    else:
        quarter=1
except Exception as err:
    quarter=1
print(quarter)


# In[6]:

#Function to generate urlLIst for origination and next quarter URLs
def generateURLList(quarter,year):
    next_quarter = quarter +1
    next_year = year +1
    urlTrainList = []
    urlTestList = []
    train_url = "https://freddiemac.embs.com/FLoan/Data/historical_data1_Q" + str(quarter)+str(year)+ ".zip"
    urlTrainList.append(train_url)
    if quarter == 4:
        quarter=1
        test_url = "https://freddiemac.embs.com/FLoan/Data/historical_data1_Q" + str(quarter)+str(next_year)+ ".zip"
        urlTestList.append(test_url)
    else:
        test_url = "https://freddiemac.embs.com/FLoan/Data/historical_data1_Q" + str(next_quarter)+str(year)+ ".zip"
        urlTestList.append(test_url) 
    return urlTrainList,urlTestList


# In[7]:

#call to function to get URLs in the list
#replace inputs by int(quarter),int(year)

urlTrainList = generateURLList(1,2005)
#urlTrainList = generateURLList(int(quarter),int(year))
print("----Generated URLs----")
for fileURL in urlTrainList[0]:
     print(fileURL)  
for fileURL in urlTrainList[1]:
     print(fileURL)        


# In[8]:

#Start session
session = requests.session()


# In[9]:

#get the login
login_url = "https://freddiemac.embs.com/FLoan/secure/auth.php"
values = dict(username= 'yamini.mait@gmail.com',password= 'Q4kzCWSk')
r = session.post(login_url,data=values)
r.status_code


# In[11]:

valuesNew={'accept':'Yes', 'acceptSubmit':'Continue', 'action':'acceptTandC'}
url='https://freddiemac.embs.com/FLoan/Data/download.php'
login=session.post(url,data=valuesNew)
page=login.content


# In[12]:

#Check cookies
cookies = r.cookies
cookies


# In[21]:

#get the geckopathdriver
#geckoPath = 'C:/Users/Yamini/Downloads/geckodriver-v0.14.0-win64/geckodriver.exe'


# In[22]:

#browser = webdriver.Firefox(executable_path=geckoPath)
#browser.get('https://freddiemac.embs.com/FLoan/secure/login.php')


# In[23]:

#username = browser.find_element_by_id("username")
#password = browser.find_element_by_id("password")
#username.send_keys("yamini.mait@gmail.com")
#password.send_keys("Q4kzCWSk")


# In[24]:

#loginForm = browser.find_element_by_name('loginform')
#loginForm.submit()


# In[25]:

#browser.find_element_by_name('accept').click(); 
#browser.find_element_by_name('acceptSubmit').click(); 
#browser
#page = browser.page_source


# In[13]:

soup = BeautifulSoup(page)


# In[14]:

tables = soup.find_all('table',attrs={'class':'table1'})
a_tags = soup.findAll('a')


# In[28]:

#quarter=1
#next_quarter = quarter +1
#year =2005
#next_year = year +1


# In[15]:

####Cleaning dataset method

def handleMissingData(df):
    #Fill date if credit_score is null
    df.Credit_Score = pd.to_numeric(df.Credit_Score,errors = 'coerce')
    df['Credit_Score'].fillna(0,inplace=True)
    df['Credit_Score'].replace(0,df.Credit_Score.mean(),inplace=True)
    #if First_Time_Homebuyer_Flag is null fill 'NotApplicable' 
    df['First_Time_Homebuyer_Flag'].fillna('Not Applicable',inplace=True)
    #If MSA is null, fill mean
    df['MSA'].fillna(df.MSA.median(),inplace=True)
    #If MI is null, just take the finite rows
    df.MI = pd.to_numeric(df.MI,errors = 'coerce')
    df['MI'].fillna(df.MI.mean(),inplace=True)
    #If Number_Of_Units is null fill mode
    df['Number_Of_Units'].fillna(df['Number_Of_Units'].mode()[0],inplace=True)
    #if Occupancy_Status is null, fill mode
    df['Occupancy_Status'].fillna(df['Occupancy_Status'].mode()[0],inplace=True)
    #if CLTV is null, fill mean
    df['CLTV'].fillna(df.CLTV.mean(),inplace=True)
    #if DTI_Ratio is null, fill mean
    df.DTI_Ratio = pd.to_numeric(df.DTI_Ratio,errors = 'coerce')
    df['DTI_Ratio'].fillna(df.DTI_Ratio.mean(),inplace=True)
    #if LTV is null, fill mean
    df['LTV'].fillna( df.LTV.mean(),inplace=True)
    #if Channel is null, fill mode
    df['Channel'].fillna(df['Channel'].mode()[0],inplace=True)
    #if PPM is null, fill mode
    df['PPM'].fillna(df['PPM'].mode()[0],inplace=True)
    #if Property_Type is null, fill mode as SF
    df['Property_Type'].fillna(df['Property_Type'].mode()[0],inplace=True)
    #If Postal_Code is null, fill 0
    df.Postal_Code.fillna(0,inplace=True)
    #If Number_Of_borrowers is null, fill mode
    df['Number_Of_Borrowers'].fillna(df['Number_Of_Borrowers'].mode()[0],inplace=True)
    #If Super_Conforming_Flag is all null, drop the column
    df = df.dropna(axis=1,how='all')
    return df


# In[16]:

#Factorize the dataset columns
def dataFactorize(df):
    #df.dropna(inplace=True)
    df['First_Time_Homebuyer_Flag'] = pd.factorize(df.First_Time_Homebuyer_Flag)[0]
    df['Occupancy_Status'] = pd.factorize(df.Occupancy_Status)[0]
    df['Channel'] = pd.factorize(df.Channel)[0]   
    df['PPM'] = pd.factorize(df.PPM)[0]
    df['Product_Type'] = pd.factorize(df.Product_Type)[0]  
    df['Property_State'] = pd.factorize(df.Property_State)[0]
    df['Property_Type'] = pd.factorize(df.Property_Type)[0]
    df['Loan_Purpose'] = pd.factorize(df.Loan_Purpose)[0]
    df['Loan_Sequence_Nmber'] = pd.factorize(df.Loan_Sequence_Nmber)[0]
    df['Service_Name'] = pd.factorize(df.Service_Name)[0]
    df['Seller_Name'] = pd.factorize(df.Seller_Name)[0]
    return df


# In[17]:

def checkPercentageOfMissingData(df):
    mis_val_percent = 100 * df.isnull().sum()/len(df)
    print(mis_val_percent)


# In[ ]:

#Get Prior Quarter Data

for fileURL in urlTrainList[0]:
    try:
        response = urllib.request.urlopen(fileURL)
        if response.getcode()==200:
            data = response.read()
            print("Reading Data")
            if zipfile.is_zipfile(io.BytesIO(data)) == True:
                print("ZipFile is valid")
                z = zipfile.ZipFile(io.BytesIO(data))
                for file in z.namelist():
                    if "historical_data1_Q" in str(file):
                        print("Get the file:"+file)
                        textFile = z.read(file)
                        df_TrainRawData = pd.read_csv(io.BytesIO(textFile),sep = '|',header=None)
                        print("File read into dataframe.This is raw Data.")
                        print(df_TrainRawData.shape)
                        df_TrainRawData.columns =['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name','Super_Conforming_Flag']
            else:
                print("[ERROR] Invalid ZIP File found at " + fileURL)
                exit()
        else:
            print("[ERROR] Invalid URL, URL( " + fileURL + " ) returned a bad HTTP response code of " + str(response.getcode()))
        response.close()
    except Exception as err:
        print("Error occured, possibly an interrupted Internet connection")
        exit()


# In[ ]:

#Get Next Quarter Data
for fileURL in urlTrainList[1]:
    try:
        response = urllib.request.urlopen(fileURL)
        if response.getcode()==200:
            data = response.read()
            print("Reading Data")
            if zipfile.is_zipfile(io.BytesIO(data)) == True:
                print("ZipFile is valid")
                z = zipfile.ZipFile(io.BytesIO(data))
                for file in z.namelist():
                    if "historical_data1_Q" in str(file):
                        print("Get the file:"+file)
                        textFile = z.read(file)
                        df_TestRawData = pd.read_csv(io.BytesIO(textFile),sep = '|',header=None)
                        print(df_TrainRawData.shape)
                        df_TestRawData.columns =['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name','Super_Conforming_Flag']
            else:
                print("[ERROR] Invalid ZIP File found at " + fileURL)
        else:
            print("[ERROR] Invalid URL, URL( " + fileURL + " ) returned a bad HTTP response code of " + str(response.getcode()))
        response.close()
    except Exception as err:
        print("Error occured, possibly an interrupted Internet connection")


# In[862]:

#create csv file with raw data 
df_TrainRawData.to_csv(priorQuarter,sep='\t', encoding='utf-8',index=False)
print("File created For Given Quarter: priorQuarter.csv")
df_TestRawData.to_csv(nextQuarter,sep='\t', encoding='utf-8',index=False)
print("File created for Next Immediate quarter: nextQuarter.csv")


# In[19]:

#Check data in both the quarters
df_TrainRawData.head()


# In[20]:

df_TestRawData.head()


# In[21]:

#Check Summary statistics
df_TrainRawData.describe()


# In[22]:

#Check correlation matrix
df_TrainRawData.corr()


# In[23]:

#Create a dataframe on which we will form our models and do analysis
df_PriorQuarter = pd.DataFrame(df_TrainRawData,columns=['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name'])


# In[24]:

df_PriorQuarter.head()


# In[25]:

#Just checking the datatypes
df_PriorQuarter.dtypes


# In[26]:

#get the data for testing dataset into some other dataframe
df_NextQuarter = pd.DataFrame(df_TestRawData,columns=['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name'])


# In[27]:

df_NextQuarter.head()


# In[28]:

#Now we have Training:df_PriorQuarter  and Testing:df_NextQuarter dataset ready
####################Data Cleansing############################


# In[29]:

#Check percentage of missing values in all the columns of given quarter
print("percentage of missing values in all the columns of given quarter")
checkPercentageOfMissingData(df_PriorQuarter)
checkPercentageOfMissingData(df_NextQuarter)


# In[30]:

#Call method for data cleansing
handleMissingData(df_PriorQuarter)


# In[31]:

handleMissingData(df_NextQuarter)


# In[32]:

#Create csv files with clean data
df_PriorQuarter.to_csv(priorQuarterCleanData,sep='\t', encoding='utf-8',index=False)
print("File created For Given Quarter: priorQuarterCleanData.csv")
df_NextQuarter.to_csv(nextQuarterCleanData,sep='\t', encoding='utf-8',index=False)
print("File created for Next Immediate quarter: nextQuarterCleanData.csv")


# In[33]:

#Check if there is any null?
checkPercentageOfMissingData(df_PriorQuarter)


# In[34]:

checkPercentageOfMissingData(df_NextQuarter)


# In[35]:

# Create list for Model metrics we will generate
Model_Metrics = []


# In[36]:

#Linear Regression Model


# In[37]:

get_ipython().magic('matplotlib inline')
plt.rcParams['figure.figsize'] = (8, 6)
plt.rcParams['font.size'] = 14


# In[47]:

df_PriorQuarter.plot(kind='scatter', x='Credit_Score', y='Interest_Rate', alpha=0.2)


# In[37]:

df_PriorQuarter.plot(kind='scatter', x='Maturity_Date', y='Interest_Rate', alpha=0.2)


# In[38]:

df_PriorQuarter.plot(kind='scatter', x='MI', y='Interest_Rate', alpha=0.2)


# In[48]:

#Get Data values for building linear model
dataFactorize(df_PriorQuarter)
dataFactorize(df_NextQuarter)
Linearfeature_cols = df_PriorQuarter.columns[df_PriorQuarter.columns.str.startswith('S') == False].drop('Interest_Rate')
Lineartest_cols = df_NextQuarter.columns[df_NextQuarter.columns.str.startswith('S') == False].drop('Interest_Rate')
X_Ltrain = df_PriorQuarter[Linearfeature_cols]
y_Ltrain = df_PriorQuarter.Interest_Rate
X_Ltest = df_NextQuarter[Lineartest_cols]
y_Ltest = df_NextQuarter.Interest_Rate


# In[559]:

#Choosing features : Credit_Score,Maturity_date,Mi,Occupancy_Status,Original_UPB,Original_Loan_Term
X_L1train = df_PriorQuarter[[0,3,5,7,10,21]]
y_L1train = df_PriorQuarter.Interest_Rate
X_L1test = df_NextQuarter[[0,3,5,7,10,21]]
y_L1test = df_NextQuarter.Interest_Rate

#0,2,3,5,6,10


# In[560]:

def linearRegression(X_Ltrain,y_Ltrain):
    print("Linear Regression model computation starts")
    lm_model=linear_model.LinearRegression()
    lm_model.fit(X_Ltrain,y_Ltrain)
    print('Linear Regression model computation has completed')
    print(lm_model.coef_)
    print(lm_model.intercept_)
    train_pred = lm_model.predict(X_Ltrain)
    print("R-Square",r2_score(y_Ltrain,train_pred))
    return(lm_model,'Linear Regression')
def predictAndEvaluate(model,X_Ltest,y_Ltest):
    test_pred = model.predict(X_Ltest)
    MAE = mean_absolute_error(y_Ltest,test_pred)
    MSE = mean_squared_error(y_Ltest,test_pred)
    MedianAE = median_absolute_error(y_Ltest,test_pred)
    MAPE = np.mean(np.abs((y_Ltest - test_pred) / y_Ltest)) * 100
    RMSE = np.sqrt(mean_squared_error(y_Ltest, test_pred))
    #plt.scatter(y_Ltest,test_pred)
    #plt.xlabel("Interest_Rate")
    #plt.ylabel("Predicted Interest_Rate")
    #plt.show()
    return (test_pred,RMSE,MAE,MAPE)    


# In[45]:

lm_1,algo_name = linearRegression(X_Ltrain,y_Ltrain)
predictAndEvaluate(lm_1,X_Ltest,y_Ltest)


# In[561]:

lm_2,algo_name = linearRegression(X_L1train,y_L1train)
test_pred,RMSE,MAE,MAPE = predictAndEvaluate(lm_2,X_L1test,y_L1test)


# In[562]:

MAPE


# In[564]:

Model_Metrics.append((algo_name,RMSE,MAE,MAPE,lm_2))


# In[565]:

Model_Metrics


# In[64]:

###Cross validation
#feature_cols = ['Credit_Score', 'Original_Loan_Term', 'Maturity_Date', 'Original_UPB']
#X = df_PriorQuarter[feature_cols]
#y = df_PriorQuarter.Interest_Rate
def train_test_rmse(feature_cols):
    X = df_PriorQuarter[feature_cols]
    y = df_PriorQuarter.Interest_Rate
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=123)
    linreg = LinearRegression()
    linreg.fit(X_train, y_train)
    X_pred = linreg.predict(X_train)
    y_pred = linreg.predict(X_test)
    return np.mean(np.abs((y_test-y_pred)/y_test))*100,metrics.r2_score(y_train, X_pred),metrics.mean_absolute_error(y_test,y_pred),np.sqrt(metrics.mean_squared_error(y_test, y_pred))
                        


# In[65]:

# compare different sets of features
print(train_test_rmse(['Credit_Score', 'Original_Loan_Term', 'First_Payment_Date', 'Original_UPB']))
print(train_test_rmse(['Credit_Score', 'Original_Loan_Term', 'Maturity_Date', 'Original_UPB']))
print(train_test_rmse(['Credit_Score', 'Original_Loan_Term', 'Maturity_Date','Number_Of_Units']))
print(train_test_rmse(['Credit_Score', 'Original_Loan_Term','Maturity_Date']))
print(train_test_rmse(['Credit_Score', 'Original_UPB','Maturity_Date']))
print(train_test_rmse(['Credit_Score', 'Original_UPB']))
print(train_test_rmse(['DTI_Ratio', 'Maturity_Date']))
print(train_test_rmse(['Credit_Score', 'Original_Loan_Term']))
print(train_test_rmse(['Original_UPB','Maturity_Date','Number_Of_Units']))


# In[66]:

feature_cols = ['Credit_Score','Original_UPB','Number_Of_Units','Maturity_Date','First_Payment_Date']
fig, axs = plt.subplots(1, len(feature_cols), sharey=True)
for index, feature in enumerate(feature_cols):
    df_PriorQuarter.plot(kind='scatter', x=feature, y='Interest_Rate', ax=axs[index], figsize=(16, 3))


# In[71]:

###Build model to predict values for credit_score
# create X and y
feature_cols = ['DTI_Ratio']
X_cr = df_PriorQuarter[feature_cols]
y_cr = df_PriorQuarter.Credit_Score


# In[72]:

lm = LinearRegression()
lm.fit(X_cr, y_cr)


# In[73]:

# print the coefficients
print(lm.intercept_)
print(lm.coef_)


# In[76]:

sns.lmplot(x='DTI_Ratio', y='Credit_Score', data=df_PriorQuarter, aspect=1.5, scatter_kws={'alpha':0.2})


# In[77]:

#Feature Selection
from sklearn.feature_selection import RFE


# In[81]:

df_new = df_PriorQuarter[[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]]
df_y = df_PriorQuarter[[12]]


# In[82]:

lmReg = LinearRegression()
rfe = RFE(estimator=lmReg, n_features_to_select=3, step=1)


# In[83]:

rfe.fit(df_new,df_y)


# In[84]:

#ranking = rfe.ranking_.reshape(digits.images[0].shape)
ranking = rfe.ranking_


# In[85]:

print(sorted(zip(map(lambda x: round(x, 2), rfe.ranking_), df_new.columns)))


# In[495]:

#Ordinary Least Squares Assumptions 
ols_model = ols("Interest_Rate ~Credit_Score+Maturity_Date+MI+Original_Loan_Term+Occupancy_Status+Original_UPB", data=df_PriorQuarter).fit()



# In[496]:

ols_model_summary = ols_model.summary()


# In[497]:

HTML(
ols_model_summary\
.as_html()\
.replace(' Adj. R-squared: ', ' Adj. R-squared: ')\
.replace('coef', 'coef')\
.replace('std err', 'std err')\
.replace('P>|t|', 'P>|t|')\
.replace('[95.0% Conf. Int.]', '[95.0% Conf. Int.]')
)


# In[498]:

fig = plt.figure(figsize=(20,12))
fig = sm.graphics.plot_partregress_grid(ols_model, fig=fig)


# In[89]:

#Random Forest


# In[567]:

#AFter checking variable importance
dataFactorize(df_PriorQuarter)
dataFactorize(df_NextQuarter)
X_R1train = df_PriorQuarter[[0,3,10,20]]
y_R1train = df_PriorQuarter.Interest_Rate
X_R1test = df_NextQuarter[[0,3,10,20]]
y_R1test = df_NextQuarter.Interest_Rate


# In[568]:

dataFactorize(df_PriorQuarter)
dataFactorize(df_NextQuarter)
test_cols = df_NextQuarter.columns[df_NextQuarter.columns.str.startswith('X') == False].drop('Interest_Rate')
feature_cols = df_PriorQuarter.columns[df_PriorQuarter.columns.str.startswith('X') == False].drop('Interest_Rate')
X_Rtrain = df_PriorQuarter[feature_cols]
y_Rtrain = df_PriorQuarter.Interest_Rate
X_Rtest = df_NextQuarter[test_cols]
y_Rtest = df_NextQuarter.Interest_Rate


# In[569]:

def randomForestRegression(X_Rtrain,y_Rtrain):
    rForest = RandomForestRegressor(n_estimators=100, max_features=4, oob_score=True, random_state=1)
    rForest.fit(X_Rtrain, y_Rtrain)
    #print(pd.DataFrame({'feature':feature_cols, 'importance':rForest.feature_importances_}).sort('importance'))
    return (rForest,'Random Forest')
def randomModelEvaluate(model,X_Rtest,y_Rtest):
    pred = model.predict(X_Rtest)
    MSE = mean_squared_error(pred, y_Rtest)
    randomRMSE = np.sqrt(mean_squared_error(y_Rtest, pred))
    randomMAE = sum(abs(y_Rtest-pred)) / len(y_Rtest)
    randomMAPE = np.mean(np.abs((y_Rtest - pred) / y_Rtest)) * 100
    return (pred,randomRMSE,randomMAE,randomMAPE)


# In[95]:

#Using all the variables
rForest,ralgo_name = randomForestRegression(X_Rtrain,y_Rtrain)
pred, randomRMSE,randomMAE,randomMAPE = randomModelEvaluate(rForest,X_Rtest,y_Rtest)


# In[570]:

#Using selected features: Credit_Score, Original_UPB, Maturity_Date, Original_Loan_Term
rForest2,ralgo_name = randomForestRegression(X_R1train,y_R1train)
pred1, randomRMSE,randomMAE,randomMAPE = randomModelEvaluate(rForest2,X_R1test,y_R1test)


# In[571]:

randomMAPE


# In[572]:

#Adding to the list
Model_Metrics.append((ralgo_name,randomRMSE,randomMAE,randomMAPE,rForest2))


# In[103]:

###KNN


# In[65]:

Knntrain = pd.read_csv(priorQuarterCleanData, sep="\t")
Knntest = pd.read_csv(nextQuarterCleanData,sep="\t")


# In[554]:

number = LabelEncoder()
Knntrain['Occupancy_Status'] = number.fit_transform(Knntrain['Occupancy_Status'].astype('str'))
Knntest['Occupancy_Status'] = number.fit_transform(Knntest['Occupancy_Status'].astype('str'))


# In[555]:

def knnRegression(Knntrain):
    # Create the knn model
    # Look at the five closest neighbors
    x_cols = ['Credit_Score','Original_Loan_Term','Maturity_Date','Original_UPB','MI','Occupancy_Status']
    y_col = ['Interest_Rate']
    knn = KNeighborsRegressor(n_neighbors=3)
    # Fit the model on the training data.
    model = knn.fit(Knntrain[x_cols],Knntrain[y_col])
    return (model,'Knn Regression')


# In[556]:

def KnnpredictAndEvaluate(Knntest,model):
    x_cols = ['Credit_Score','Original_Loan_Term','Maturity_Date','Original_UPB','MI','Occupancy_Status']
    y_col = ['Interest_Rate']
    predictions = model.predict(Knntest[x_cols])
    # Get the actual values for the test set.
    actual = Knntest[y_col]
    # Compute the mean squared error of our predictions.
    mse = (((predictions - actual) ** 2).sum()) / len(predictions) 
    knnMAE = metrics.mean_absolute_error(actual,predictions)
    knnMAPE = np.mean(np.abs((actual - predictions) / actual)) * 100
    knnRMSE = np.sqrt(mean_squared_error(actual, predictions))
    return (actual,knnRMSE,knnMAE,knnMAPE)


# In[557]:

#Run the Knn Model
Knnmodel,Kalgo_name = knnRegression(Knntrain)
actual,knnRMSE,knnMAE,knnMAPE = KnnpredictAndEvaluate(Knntest,Knnmodel)


# In[573]:

#MAPE for Knn
knnMAPE[0]


# In[650]:

#Adding to the list 
Model_Metrics.append((Kalgo_name,knnRMSE,knnMAE,knnMAPE[0],Knnmodel))


# In[107]:

##Neural-Network using h2o library


# In[575]:

h2o.init()


# In[576]:

data = h2o.import_file(priorQuarterCleanData)


# In[577]:

testH2OData = h2o.import_file(nextQuarterCleanData)


# In[578]:

data.head()


# In[824]:

#Defining columns for X and Y
y = "Interest_Rate"
#x = data.names
x = ['Credit_Score','Original_Loan_Term','Maturity_Date','Original_UPB','MI','Occupancy_Status']
#x.remove(y)


# In[580]:

trainingData = data


# In[825]:

def h2oDeepLearning(x,y):
    m = h2o.estimators.deeplearning.H2ODeepLearningEstimator()
    m.train(x, y, trainingData)
    return m


# In[826]:

def h2oPredictAndEvaluate(m,testH2OData):
    p = m.predict(testH2OData)
    #m.model_performance(testH2OData)
    perf = m.model_performance(testH2OData)
    MSE = m.mse(perf)
    RMSE = m.rmse(perf)
    MAE = m.mae(perf)
    #return (m.model_performance(testH2OData))
    return MSE,RMSE,MAE


# In[827]:

h2Omodel= h2oDeepLearning(x,y)


# In[830]:

h2omse,h2ormse,h2omae= h2oPredictAndEvaluate(h2Omodel,testH2OData)


# In[617]:

h2o_algoname = 'Neural Network'
model = 'h2Omodel'


# In[855]:

NeuralNetwork_ModelMetrics = []


# In[856]:

NeuralNetwork_ModelMetrics.append((h2o_algoname,h2omse,h2ormse,h2omae,model))


# In[858]:

df_Net_ModelMetrics = pd.DataFrame.from_records(NeuralNetwork_ModelMetrics, columns=["Algorithm", "MSE", "RMSE", "MAE","Model"])


# In[852]:

df_ModelMetrics = pd.DataFrame.from_records(Model_Metrics, columns=["Algorithm", "RMSE", "MAE", "MAPE","Model"])


# In[853]:

df_ModelMetrics['Model_Rank'] = df_ModelMetrics['MAPE'].rank(ascending=1)


# In[859]:

df_Net_ModelMetrics


# In[849]:

df_ModelMetrics.to_csv(modelMetrics,sep=',', encoding='utf-8',header=True)
print("File created For Model Metrics: ModelMetrics.csv")


# In[861]:

df_Net_ModelMetrics.to_csv(neuralNetworkMetrics,sep=',', encoding='utf-8',header=True)


# In[1]:

####Choose best model and perform further analysis


# In[ ]:

###########Financial Crisis


# In[91]:

#Function to get URL for year 2007 origination data
year2007 = 2007
year2008 = 2008
def generate2007URL(quarter):
    url2007List = []
    trn_url = "https://freddiemac.embs.com/FLoan/Data/historical_data1_Q"+str(quarter)+str(year2008)+ ".zip"
    url2007List.append(trn_url)
    return url2007List


# In[92]:


#Call to function for origination data for Year 2007
url2007List = generate2007URL(1)
print("----Generated URLs----")
for fileURL in url2007List:
     print(fileURL) 


# In[93]:

for fileURL in url2007List:
    try:
        response = urllib.request.urlopen(fileURL)
        if response.getcode()==200:
            data = response.read()
            print("Reading Data")
            if zipfile.is_zipfile(io.BytesIO(data)) == True:
                print("ZipFile is valid")
                z = zipfile.ZipFile(io.BytesIO(data))
                for file in z.namelist():
                    if "historical_data1_Q" in str(file):
                        print("Get the file:"+file)
                        textFile = z.read(file)
                        df_Q12008 = pd.read_csv(io.BytesIO(textFile),sep = '|',header=None)
                        print("File read into dataframe.This is raw Data.")
                        print(df_Q12008.shape)
                        df_Q12008.columns =['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name','Super_Conforming_Flag']
            else:
                print("[ERROR] Invalid ZIP File found at " + fileURL)
                exit()
        else:
            print("[ERROR] Invalid URL, URL( " + fileURL + " ) returned a bad HTTP response code of " + str(response.getcode()))
        response.close()
    except Exception as err:
        print("Error occured, possibly an interrupted Internet connection")
        exit()


# In[85]:

df_Q32007


# In[78]:

#create dataframes for all the quarters
df_Q12007data = pd.DataFrame(df_Q12007,columns =['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name'])


# In[104]:

#handleMissingData(df_Q12008data)
dataFactorize(df_Q12008data)


# In[185]:

Model2007_Metrics = []


# In[524]:

X_2007Train = df_Q42007data[[0,3,5,7,10,21]]
y_2007Train = df_Q42007data.Interest_Rate
X_2007Test = df_Q12008data[[0,3,5,7,10,21]]
y_2007Test = df_Q12008data.Interest_Rate


# In[525]:


def linearRegression2007(X_2007Train,y_2007Train):
    lm=linear_model.LinearRegression()
    lm.fit(X_2007Train,y_2007Train)
    print(lm.coef_)
    print(lm.intercept_)
    t_pred = lm.predict(X_2007Train)
    print("R-Square",r2_score(y_2007Train,t_pred))
    return lm
def predictAndEvaluate2007(lm,X_2007Test,y_2007Test):
    pred2007 = lm.predict(X_2007Test)
    MAE = mean_absolute_error(y_2007Test,pred2007)
    MSE = mean_squared_error(y_2007Test,pred2007)
    MAPE = np.mean(np.abs((y_2007Test - pred2007) / y_2007Test)) * 100
    RMSE = np.sqrt(mean_squared_error(y_2007Test, pred2007))
    #plt.scatter(X_2007Test, y_2007Test,  color='black')
    #plt.plot(X_2007Test, pred2007, color='blue',linewidth=3)
    #plt.xticks(())
    #plt.yticks(())
    #plt.show()
    return (pred2007,RMSE,MAE,MAPE)


# In[526]:

lm = linearRegression2007(X_2007Train,y_2007Train)
lm


# In[527]:

pred2007,RMSE,MAE,MAPE = predictAndEvaluate2007(lm,X_2007Test,y_2007Test)


# In[528]:

MAPE


# In[529]:

Model2007_Metrics.append((RMSE,MAE,MAPE,lm))


# In[530]:

Model2007_Metrics


# In[532]:

df_FinancialCrisis = pd.DataFrame.from_records(Model2007_Metrics, columns=["RMSE", "MAE", "MAPE","Model"])


# In[533]:

df_quar = pd.DataFrame({'Quarter-Period': ['Q12007-Q22007','Q22007-Q32007','Q32007-Q42007','Q42007-Q12008']})


# In[534]:

df_FinancialCrisis.join(df_quar)


# In[542]:

df_Q42007data.plot(kind='scatter', x='Original_Loan_Term', y='Interest_Rate', alpha=0.2)


# In[340]:

'''
############Year 2009################


# In[714]:

#year2009 = 2009
#year2010 = 2010
year1999 = 1999
def generate2009URL(quarter):
    url2009List = []
    trn_url = "https://freddiemac.embs.com/FLoan/Data/historical_data1_Q"+str(quarter)+str(year2009)+ ".zip"
    url2009List.append(trn_url)
    return url2009List


# In[670]:

#Call to function for origination data for Year 2007
url2009List = generate2009URL(1)
print("----Generated URLs----")
for fileURL in url2009List:
     print(fileURL) 


# In[688]:

Model2009_Metrics = []


# In[671]:

for fileURL in url2009List:
    try:
        response = urllib.request.urlopen(fileURL)
        if response.getcode()==200:
            data = response.read()
            print("Reading Data")
            if zipfile.is_zipfile(io.BytesIO(data)) == True:
                print("ZipFile is valid")
                z = zipfile.ZipFile(io.BytesIO(data))
                for file in z.namelist():
                    if "historical_data1_Q" in str(file):
                        print("Get the file:"+file)
                        textFile = z.read(file)
                        df_Q22009 = pd.read_csv(io.BytesIO(textFile),sep = '|',header=None)
                        print("File read into dataframe.This is raw Data.")
                        print(df_Q22009.shape)
                        df_Q22009.columns =['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name','Super_Conforming_Flag']
            else:
                print("[ERROR] Invalid ZIP File found at " + fileURL)
                exit()
        else:
            print("[ERROR] Invalid URL, URL( " + fileURL + " ) returned a bad HTTP response code of " + str(response.getcode()))
        response.close()
    except Exception as err:
        print("Error occured, possibly an interrupted Internet connection")
        exit()


# In[546]:

#create dataframe for all the quarters
df_Q12009data = pd.DataFrame(df_Q12009,columns =['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name'])


# In[738]:

#handleMissingData(df_Q42009data)
#dataFactorize(df_Q12009data)
df_Q42009data['Interest_Rate'].describe()


# In[732]:

df_Q12009data['Interest_Rate'].describe()


# In[707]:

#set X and Y params
X_2009Train = df_Q32009data[[0,3,5,7,10,21]]
y_2009Train = df_Q32009data.Interest_Rate
X_2009Test = df_Q42009data[[0,3,5,7,10,21]]
y_2009Test = df_Q42009data.Interest_Rate


# In[708]:

#Function
def linearRegression2009(X_2009Train,y_2009Train):
    lm2009=linear_model.LinearRegression()
    lm2009.fit(X_2009Train,y_2009Train)
    print(lm2009.coef_)
    print(lm2009.intercept_)
    t2009_pred = lm2009.predict(X_2009Train)
    print("R-Square",r2_score(y_2009Train,t2009_pred))
    return lm2009
def predictAndEvaluate2009(lm2009,X_2009Test,y_2009Test):
    pred2009 = lm.predict(X_2009Test)
    MAE2009 = mean_absolute_error(y_2009Test,pred2009)
    MSE = mean_squared_error(y_2009Test,pred2009)
    MAPE2009 = np.mean(np.abs((y_2009Test - pred2009) / y_2009Test)) * 100
    RMSE2009 = np.sqrt(mean_squared_error(y_2009Test, pred2009))
    #plt.scatter(X_2009Test, y_2009Test,  color='black')
    #plt.plot(X_2009Test, pred2009, color='blue',linewidth=3)
    #plt.xticks(())
    #plt.yticks(())
    #plt.show()
    return (pred2009,RMSE2009,MAE2009,MAPE2009)


# In[709]:

lm2009 = linearRegression2009(X_2009Train,y_2009Train)

pred2009,RMSE2009,MAE2009,MAPE2009 = predictAndEvaluate2009(lm2009,X_2009Test,y_2009Test)


# In[710]:

Model2009_Metrics.append((RMSE2009,MAE2009,MAPE2009,lm2009))


# In[706]:

Model2009_Metrics


# In[711]:

df_2009Analysis = pd.DataFrame.from_records(Model2009_Metrics, columns=["RMSE", "MAE", "MAPE","Model"])


# In[712]:

df_quar2009 = pd.DataFrame({'Quarter-Period': ['Q12009-Q22009','Q22009-Q32009','Q32009-Q42009']})


# In[713]:

df_2009Analysis.join(df_quar2009)


# In[733]:

df_Q32009data.plot(kind='scatter', x='Original_Loan_Term', y='Interest_Rate', alpha=0.2)


# In[779]:

############Economic boom############
#year1999 = 1999
year2013 = 2013
def generateEcoURL(quarter):
    urlEcoList = []
    trn_url = "https://freddiemac.embs.com/FLoan/Data/historical_data1_Q"+str(quarter)+str(year2013)+ ".zip"
    urlEcoList.append(trn_url)
    return urlEcoList


# In[791]:

#Call to function for origination data for Year 2007
urlEcoList = generateEcoURL(4)
print("----Generated URLs----")
for fileURL in urlEcoList:
     print(fileURL)


# In[792]:

for fileURL in urlEcoList:
    try:
        response = urllib.request.urlopen(fileURL)
        if response.getcode()==200:
            data = response.read()
            print("Reading Data")
            if zipfile.is_zipfile(io.BytesIO(data)) == True:
                print("ZipFile is valid")
                z = zipfile.ZipFile(io.BytesIO(data))
                for file in z.namelist():
                    if "historical_data1_Q" in str(file):
                        print("Get the file:"+file)
                        textFile = z.read(file)
                        df_Q42013 = pd.read_csv(io.BytesIO(textFile),sep = '|',header=None)
                        print("File read into dataframe.This is raw Data.")
                        print(df_Q42013.shape)
                        df_Q42013.columns =['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name','Super_Conforming_Flag']
            else:
                print("[ERROR] Invalid ZIP File found at " + fileURL)
                exit()
        else:
            print("[ERROR] Invalid URL, URL( " + fileURL + " ) returned a bad HTTP response code of " + str(response.getcode()))
        response.close()
    except Exception as err:
        print("Error occured, possibly an interrupted Internet connection")
        exit()


# In[793]:

#create dataframe for all the quarters
df_Q42013data = pd.DataFrame(df_Q42013,columns =['Credit_Score', 'First_Payment_Date', 'First_Time_Homebuyer_Flag','Maturity_Date',
                                                   'MSA','MI','Number_Of_Units','Occupancy_Status','CLTV','DTI_Ratio',
                                                   'Original_UPB','LTV','Interest_Rate','Channel','PPM','Product_Type',
                                                   'Property_State','Property_Type','Postal_Code','Loan_Sequence_Nmber',
                                                   'Loan_Purpose','Original_Loan_Term','Number_Of_Borrowers','Seller_Name',
                                                    'Service_Name'])


# In[801]:

#handleMissingData(df_Q42013data)
dataFactorize(df_Q42013data)
#df_Q32013data


# In[805]:

Model2013_Metrics = []


# In[814]:

#set c and y param
#1999
#X_1999Train = df_Q21999data[[0,3,5,7,10,21]]
#y_1999Train = df_Q21999data.Interest_Rate
#X_1999Test = df_Q31999data[[0,3,5,7,10,21]]
#y_1999Test = df_Q31999data.Interest_Rate

#2013
X_2013Train = df_Q32013data[[0,3,5,7,10,21]]
y_2013Train = df_Q32013data.Interest_Rate
X_2013Test = df_Q42013data[[0,3,5,7,10,21]]
y_2013Test = df_Q42013data.Interest_Rate


# In[809]:


def linearRegression1999(X_1999Train,y_1999Train):
    lm=linear_model.LinearRegression()
    lm.fit(X_1999Train,y_1999Train)
    print(lm.coef_)
    print(lm.intercept_)
    t_pred = lm.predict(X_1999Train)
    print("R-Square",r2_score(y_1999Train,t_pred))
    return lm
def predictAndEvaluate1999(lm,X_1999Test,y_1999Test):
    pred1999 = lm.predict(X_1999Test)
    MAE = mean_absolute_error(y_1999Test,pred1999)
    MSE = mean_squared_error(y_1999Test,pred1999)
    MAPE = np.mean(np.abs((y_1999Test - pred1999) / y_1999Test)) * 100
    RMSE = np.sqrt(mean_squared_error(y_1999Test, pred1999))
    return (pred1999,RMSE,MAE,MAPE)


# In[815]:

def linearRegression2013(X_2013Train,y_2013Train):
    lm=linear_model.LinearRegression()
    lm.fit(X_2013Train,y_2013Train)
    print(lm.coef_)
    print(lm.intercept_)
    t_pred = lm.predict(X_2013Train)
    print("R-Square",r2_score(y_2013Train,t_pred))
    return lm
def predictAndEvaluate2013(lm,X_2013Test,y_2013Test):
    pred2013 = lm.predict(X_2013Test)
    MAE = mean_absolute_error(y_2013Test,pred2013)
    MSE = mean_squared_error(y_2013Test,pred2013)
    MAPE = np.mean(np.abs((y_2013Test - pred2013) / y_2013Test)) * 100
    RMSE = np.sqrt(mean_squared_error(y_2013Test, pred2013))
    return (pred2013,RMSE,MAE,MAPE)


# In[816]:

lm2013 = linearRegression2013(X_2013Train,y_2013Train)

pred2013,RMSE2013,MAE2013,MAPE2013 = predictAndEvaluate2013(lm2013,X_2013Test,y_2013Test)


# In[817]:

Model2013_Metrics.append((RMSE2013,MAE2013,MAPE2013,lm2013))


# In[818]:

Model2013_Metrics


# In[821]:

df_2013Analysis = pd.DataFrame.from_records(Model2013_Metrics, columns=["RMSE", "MAE", "MAPE","Model"])


# In[822]:

df_2013Analysis

