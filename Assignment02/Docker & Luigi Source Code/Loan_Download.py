
# coding: utf-8

# In[1]:

import urllib
from urllib.request import urlopen
from bs4 import BeautifulSoup
import zipfile, io
import pandas as pd
from time import strptime
from datetime import datetime
from sklearn import linear_model
from sklearn.linear_model import LinearRegression


# In[4]:

def listOfDownloadLinks(selectOptions,fileNames):
    list_year_url = list()
    urlFirstPart = 'https://resources.lendingclub.com/'
    for i in range(0,len(selectOptions)):
        fullURL = urlFirstPart + fileNames[i]
        list_year_url.append((selectOptions[i],fullURL))
    return list_year_url


# In[5]:

def generateOptionsListAndFileNamesList(selectElementTag,fileNamesElement):    
    year_list = list()
    for a in selectElementTag.select("option"):
        year_list.append(a.text.strip())
    fileNames  = fileNamesElement.text.strip().split("|")
    return (year_list,fileNames)


# In[7]:

def downloadLoanDataSet(fileName):
    print('=====================')
    print(fileName)
    print('Inside downloadLoanDataSet')
    print('=====================')
    url = 'https://www.lendingclub.com/info/download-data.action'
    page = urlopen(url)
    soup = BeautifulSoup(page,"lxml")
    select_loan=soup.find("select", {"id":"loanStatsDropdown"})
    loan=soup.find("div",{"id":"loanStatsFileNamesJS"})
    select_loan_list,loan_fileNames = generateOptionsListAndFileNamesList(select_loan, loan)
    loan_downloadURL_list = listOfDownloadLinks(select_loan_list,loan_fileNames)
    pathOutputFile = fileName
    loanDataSetdtype = {"id": object, "next_pymnt_d": object, "desc": object, "verification_status_joint": object}
    appendWrite = True
    for data in loan_downloadURL_list:
        #try:
            year, fileURL = data
            print(fileURL)
            response = urllib.request.urlopen(fileURL)
            if response.getcode()==200:
                data = response.read()
                if zipfile.is_zipfile(io.BytesIO(data)) == True:
                    z = zipfile.ZipFile(io.BytesIO(data))
                    for file in z.namelist():
                        if file.endswith('.csv'):
                            if appendWrite:
                                csvFile = z.read(file)
                                df = pd.read_csv(io.BytesIO(csvFile),header=0,skiprows=[0], dtype = loanDataSetdtype)
                                df = df[:-2]
                                df.to_csv(pathOutputFile,header ='column_names')
                                appendWrite = False
                                print('After First Iteration')
                                print(df.shape)
                            else:
                                csvFile = z.read(file)
                                df_new = pd.read_csv(io.BytesIO(csvFile),header=0,skiprows=[0], dtype = loanDataSetdtype)
                                df_new = df_new[:-2]
                                df_new.to_csv(pathOutputFile,mode = 'a',header=False)
                                df = pd.concat([df,df_new])
                                print("Total rows and columns in the data files tables");
                                print(df.shape)
                                # return df
        #except Exception as err:
            #print("Error occured, possibly an interrupted Internet connection")
            #logger.info("Error occured, possibly an interrupted Internet connection")


# In[8]:

# downloadLoanDataSet()


# In[11]:

def downloadRejectLoanDataSet(fileName):
    print('=====================')
    print(fileName)
    print('Inside downloadRejectLoanDataSet')
    print('=====================')
    url = 'https://www.lendingclub.com/info/download-data.action'
    page = urlopen(url)
    soup = BeautifulSoup(page,"lxml")
    select_reject=soup.find("select", {"id":"rejectStatsDropdown"})
    reject=soup.find("div",{"id":"rejectedLoanStatsFileNamesJS"})
    select_reject_list,reject_loan_fileNames = generateOptionsListAndFileNamesList(select_reject, reject)
    reject_downloadURL_list = listOfDownloadLinks(select_reject_list,reject_loan_fileNames)
    pathOutputFile = fileName
    appendWrite = True
    for data in reject_downloadURL_list:
        #try:
            year, fileURL = data
            print(fileURL)
            response = urllib.request.urlopen(fileURL)
            if response.getcode()==200:
                data = response.read()
                if zipfile.is_zipfile(io.BytesIO(data)) == True:
                    z = zipfile.ZipFile(io.BytesIO(data))
                    for file in z.namelist():
                        if file.endswith('.csv'):
                            if appendWrite:
                                csvFile = z.read(file)
                                df_rej = pd.read_csv(io.BytesIO(csvFile),header=0,skiprows=[0])
                                df_rej.to_csv(pathOutputFile,header ='column_names')
                                appendWrite = False
                                print('After First Iteration')
                                print(df_rej.shape)
                            else:
                                csvFile = z.read(file)
                                df_new = pd.read_csv(io.BytesIO(csvFile),header=0,skiprows=[0])
                                #df_new = df_new[:-2]
                                df_new.to_csv(pathOutputFile,mode = 'a',header=False)
                                df_rej = pd.concat([df_rej,df_new])
                                print("Total rows and columns in the data files tables");
                                print(df_rej.shape)
                                # return df_rej
        #except Exception as err:
            #print("Error occured, possibly an interrupted Internet connection")
            #logger.info("Error occured, possibly an interrupted Internet connection")

#Rename columns
def renameColumns(df):
    # df.rename(columns={'Amount Requested': 'Amount_Requested'}, inplace=True)
    # df.rename(columns={'Employment Length': 'Employment_Length'}, inplace=True)
    # df.rename(columns={'Loan Title': 'Loan_Title'}, inplace=True)
    # df.rename(columns={'Application Date': 'Application_Date'}, inplace=True)
    # df.rename(columns={'Policy Code': 'Policy_Code'}, inplace=True)
    # df.rename(columns={'Zip Code': 'Zip_Code'}, inplace=True)
    # df.rename(columns={'Debt-To-Income Ratio': 'Debt-To-Income-Ratio'}, inplace=True)
    df.rename(columns={'Amount Requested': 'Amount_Requested'}, inplace=True)
    df.rename(columns={'Employment Length': 'Employment_Length'}, inplace=True)
    df.rename(columns={'Loan Title': 'Loan_Title'}, inplace=True)
    df.rename(columns={'Application Date': 'Application_Date'}, inplace=True)
    df.rename(columns={'Policy Code': 'Policy_Code'}, inplace=True)
    df.rename(columns={'Zip Code': 'Zip_Code'}, inplace=True)
    df.rename(columns={'Debt-To-Income Ratio': 'Debt-To-Income-Ratio'}, inplace=True)
    return df

#Handle missing data
def HandleRejectsMissingData(df):
    print(df.dtypes)
    # df['Employment_Length'] = df.Employment_Length.str.replace('years','')
    # df['Employment_Length'] = df.Employment_Length.str.replace('year','')
    # df['Employment_Length'] = df.Employment_Length.str.replace('+','')
    # df['Employment_Length'] = df.Employment_Length.str.replace('< 1','0')
    # df['Employment_Length'].fillna(0, inplace=True)
    # df['Employment_Length']=pd.to_numeric(df['Employment_Length'],errors='coerce')
    # df['Debt-To-Income-Ratio']=df['Debt-To-Income-Ratio'].str.split('%',1).str[0]
    # df['Debt-To-Income-Ratio']=pd.to_numeric(df['Debt-To-Income-Ratio'])
    # df['Amount_Requested'].fillna(df['Amount_Requested'].mean(), inplace=True)
    # df['Debt-To-Income-Ratio'].fillna(df['Debt-To-Income-Ratio'].mean(), inplace=True)
    # df['Loan_Title'].fillna('Unknown', inplace=True)
    # df['Risk_Score'].replace(-1,0,inplace=True)
    df['Employment_Length'] = df.Employment_Length.str.replace('years', '')
    df['Employment_Length'] = df.Employment_Length.str.replace('year', '')
    df['Employment_Length'] = df.Employment_Length.str.replace('+', '')
    df['Employment_Length'] = df.Employment_Length.str.replace('< 1', '0')
    df['Employment_Length'].fillna(0, inplace=True)
    df['Employment_Length'] = pd.to_numeric(df['Employment_Length'], errors='coerce')
    df['Risk_Score_Mean'] = df.Risk_Score
    df.Risk_Score_Mean.fillna(df.Risk_Score.mean(), inplace=True)
    df['Risk_Score_Median'] = df.Risk_Score
    df.Risk_Score_Median.fillna(df.Risk_Score.median(), inplace=True)
    df['Debt-To-Income-Ratio'] = df['Debt-To-Income-Ratio'].str.split('%', 1).str[0]
    df['Debt-To-Income-Ratio'] = pd.to_numeric(df['Debt-To-Income-Ratio'])
    df['Amount_Requested'].fillna(df['Amount_Requested'].mean(), inplace=True)
    df['Debt-To-Income-Ratio'].fillna(df['Debt-To-Income-Ratio'].mean(), inplace=True)
    df['Loan_Title'].fillna('Unknown', inplace=True)
    return df

def dropNullColumns(df):
    half_count=len(df)/2
    df=df.dropna(thresh=half_count,axis=1)
    return df

def getUsefuldata(df):
    print(df.dtypes)
    # df['id']=pd.to_numeric(df['id'],errors='coerce')
    df.int_rate=df.int_rate.str.split('%',1).str[0]
    df.int_rate=pd.to_numeric(df.int_rate, errors='coerce')
    df.revol_util=df.revol_util.str.split('%',1).str[0]
    df.revol_util=pd.to_numeric(df.revol_util,errors='coerce')
    df.term=df.term.str.split(' ',2).str[1]
    df.term=pd.to_numeric(df.term, errors='coerce')
    # df=df[~(df['id'].isnull())]
    df=df[~(df.delinq_2yrs.isnull())]
    return df

#Missing data function
def HandleMissingData(df):
    df['emp_title'].fillna('Job Title not given', inplace=True)
    df['mths_since_last_delinq'].fillna(df['mths_since_last_delinq'].max(),inplace=True)
    df.annual_inc.fillna(df.annual_inc.mean(), inplace=True)
    df.delinq_2yrs.fillna(df.delinq_2yrs.median(), inplace=True)
    df.title.fillna(df.purpose, inplace=True)
    df.earliest_cr_line.fillna(df.earliest_cr_line.mode(), inplace=True)
    df.inq_last_6mths.fillna(df.inq_last_6mths.mean(),inplace=True)
    df.mths_since_last_delinq.fillna(df.mths_since_last_delinq.max(), inplace=True)
    df.revol_util.fillna(df.revol_util.median(), inplace=True)
    df.last_pymnt_d.fillna(df.last_pymnt_d.mode(), inplace=True)
    df.last_pymnt_d.fillna(method='bfill', inplace=True)
    df.last_pymnt_d.fillna(method='ffill', inplace=True)
    # df.next_pymnt_d.fillna("Dec-9999", inplace = True)
    df.last_credit_pull_d.fillna(df.last_credit_pull_d.mode(), inplace = True)
    df.last_credit_pull_d.fillna(method='bfill', inplace = True)
    df.last_credit_pull_d.fillna(method='ffill', inplace = True)
    df.collections_12_mths_ex_med.fillna(0, inplace=True)
    df.tot_coll_amt.fillna(df.tot_coll_amt.median(), inplace=True)
    df.tot_cur_bal.fillna(df.tot_cur_bal.median(), inplace=True)
    df.total_rev_hi_lim.fillna(df.total_rev_hi_lim.median(), inplace=True)
    df.avg_cur_bal.fillna(df.avg_cur_bal.mean(), inplace=True)
    df.bc_open_to_buy.fillna(df.bc_open_to_buy.mean(), inplace=True)
    df.bc_util.fillna((df.tot_cur_bal/df.total_rev_hi_lim), inplace=True)
    df.bc_util.fillna(0, inplace=True)
    df.chargeoff_within_12_mths.fillna(0, inplace=True)
    df.mo_sin_old_il_acct.fillna(df.mo_sin_old_il_acct.max(), inplace=True)
    df.mo_sin_old_rev_tl_op.fillna(df.mo_sin_old_rev_tl_op.max(), inplace=True)
    df.mo_sin_rcnt_rev_tl_op.fillna(df.mo_sin_rcnt_rev_tl_op.min(), inplace=True)
    df.mo_sin_rcnt_tl.fillna(df.mo_sin_rcnt_tl.min(), inplace=True)
    df.mort_acc.fillna(df.mort_acc.mean(), inplace=True)
    df.mths_since_recent_bc.fillna(df.mths_since_recent_bc.min(), inplace=True)
    df.mths_since_recent_inq.fillna(df.mths_since_recent_inq.min(), inplace=True)
    df.num_accts_ever_120_pd.fillna(df.num_accts_ever_120_pd.median(), inplace=True)
    df.num_actv_bc_tl.fillna(df.num_actv_bc_tl.median(), inplace=True)
    df.num_actv_rev_tl.fillna(df.num_actv_rev_tl.median(), inplace=True)
    df.num_bc_sats.fillna(df.num_bc_sats.median(), inplace=True)
    df.num_bc_tl.fillna(df.num_bc_tl.median(), inplace=True)
    df.num_il_tl.fillna(df.num_il_tl.median(), inplace=True)
    df.num_op_rev_tl.fillna(df.num_op_rev_tl.median(), inplace=True)
    df.num_rev_accts.fillna(df.num_rev_accts.median(), inplace=True)
    df.num_rev_tl_bal_gt_0.fillna(df.num_rev_tl_bal_gt_0.median(), inplace=True)
    df.num_sats.fillna(df.num_sats.median(), inplace=True)
    df.num_tl_120dpd_2m.fillna(df.num_tl_120dpd_2m.median(), inplace=True)
    df.num_tl_30dpd.fillna(df.num_tl_30dpd.median(), inplace=True)
    df.num_tl_90g_dpd_24m.fillna(df.num_tl_90g_dpd_24m.median(), inplace=True)
    df.num_tl_op_past_12m.fillna(df.num_tl_op_past_12m.median(), inplace=True)
    df.pct_tl_nvr_dlq.fillna(df.pct_tl_nvr_dlq.median(), inplace=True)
    df.percent_bc_gt_75.fillna(df.percent_bc_gt_75.median(), inplace=True)
    df.pub_rec_bankruptcies.fillna(df.pub_rec_bankruptcies.median(), inplace=True)
    df.tax_liens.fillna(df.tax_liens.median(), inplace=True)
    df.tot_hi_cred_lim.fillna(df.tot_hi_cred_lim.median(), inplace=True)
    df.total_bal_ex_mort.fillna(df.tot_cur_bal, inplace=True)
    df.total_bc_limit.fillna(df.total_bc_limit.median(), inplace=True)
    df.total_il_high_credit_limit.fillna(df.total_il_high_credit_limit.median(), inplace=True)
    df.acc_open_past_24mths.fillna(df.acc_open_past_24mths.median(), inplace=True)
    df.issue_d.fillna(df.issue_d.mode(),inplace=True)
    return df

def DeriveMonth(monthname):
    month=[]
    i=0
    while i < len(monthname):
        x=strptime(str(monthname.iloc[i]),'%b').tm_mon
        month.append(x)
        i=i+1
    return month


def DeriveMonthYear(df):
    issuemonthname = df.issue_d.str.split('-', 1).str[0]
    issueyearname = df.issue_d.str.split('-', 1).str[1]
    issuemonth = DeriveMonth(issuemonthname)
    issueyear = pd.to_numeric(issueyearname)
    df['issue_d_year'] = issueyear
    df['issue_d_month'] = issuemonth
    df.issue_d_monthyear = df['issue_d_month'].map(str) + "/" + df['issue_d_year'].map(str)

    crmonthname = df.earliest_cr_line.str.split('-', 1).str[0]
    cryearname = df.earliest_cr_line.str.split('-', 1).str[1]
    crmonth = DeriveMonth(crmonthname)
    cryear = pd.to_numeric(cryearname)
    df['earliest_cr_line_year'] = cryear
    df['earliest_cr_line_month'] = crmonth
    df.earliest_cr_line_monthyear = df['earliest_cr_line_month'].map(str) + "/" + df['earliest_cr_line_year'].map(str)

    lastPymntMonthName = df.last_pymnt_d.str.split('-', 1).str[0]
    lastPymntYearName = df.last_pymnt_d.str.split('-', 1).str[1]
    lastPymntMonth = DeriveMonth(lastPymntMonthName)
    lastPymntYear = pd.to_numeric(lastPymntYearName)
    df['last_pymnt_month'] = lastPymntMonth
    df['last_pymnt_year'] = lastPymntYear
    df['last_pymnt_monthYear'] = df['last_pymnt_month'].map(str) + "/" + df['last_pymnt_year'].map(str)

    # nextPymntMonthName = df.next_pymnt_d.str.split('-', 1).str[0]
    # nextPymntYearName = df.next_pymnt_d.str.split('-', 1).str[1]
    # nextPymntMonth = DeriveMonth(nextPymntMonthName)
    # nextPymntYear = pd.to_numeric(nextPymntYearName)
    # df['next_pymnt_month'] = nextPymntMonth
    # df['next_pymnt_year'] = nextPymntYear
    # df['next_pymnt_monthYear'] = df['next_pymnt_month'].map(str) + "/" + df['next_pymnt_year'].map(str)

    lastCreditMonthName = df.last_credit_pull_d.str.split('-', 1).str[0]
    lastCreditYearName = df.last_credit_pull_d.str.split('-', 1).str[1]
    lastCreditMonth = DeriveMonth(lastCreditMonthName)
    lastCreditYear = pd.to_numeric(lastCreditYearName)
    df['last_credit_pull_month'] = lastCreditMonth
    df['last_credit_pull_year'] = lastCreditYear
    df['last_credit_pull_monthYear'] = df['last_credit_pull_month'].map(str) + "/" + df['last_credit_pull_year'].map(
        str)

    return df

def DeriveNumericColumns(df):
    df['grade_num'] = df['grade'].map({'A':7,'B':6,'C':5,'D':4,'E':3,'F':2,'G':1})
    df['sub_grade_num'] = df['sub_grade'].map({'A1':35,'A2':34,'A3':33,'A4':32,'A5':31,'B1':30,'B2:':29,'B3':28,'B4':27,'B5':26,'C1':25,'C2':24,'C3':23,'C4':22,'C5':21,'D1':20,'D2':19,'D3':18,'D4':17,'D5':16,'E1':15,'E2':14,'E3':13,'E4':12,'E5':11,'F1':10,'F2':9,'F3':8,'F4':7,'F5':6,'G1':5,'G2':4,'G3':3,'G4':2,'G5':1})
    df['home_ownership_num'] = df['home_ownership'].map({'RENT':1,'OWN':2,'MORTGAGE':3,'OTHER':4,'NONE':5,'ANY':6})
    df['verification_status_num'] = df['verification_status'].map({'Verified':1,'Source Verified':1,'Not Verified':2})
    df['pymnt_plan_clean'] = df['pymnt_plan'].map({'n':1,'y':2})
    df['purpose_num'] = df['purpose'].map({'credit_card':1,'car':2,'small_business':3,'other':4,'wedding':5,'debt_consolidation':6,'home_improvement':7,'major_purchase':8,'medical':9,'moving':10,'vacation':11,'house':12,'renewable_energy':13,'educational':14})
    df['application_type_num'] = df['application_type'].map({'INDIVIDUAL':1,'JOINT':2,'DIRECT_PAY':3})
    df['loan_status_num'] = df['loan_status'].map({'Current': 1, 'Fully Paid': 2, 'Charged Off':3, 'Late(31-120 days)':4, 'In Grace Period': 5, 'Late(16-30 days)': 6, 'Default': 7,'Does not meet the credit policy. Status:Fully Paid':8,'Does not meet the credit policy. Status:Charged Off':9})
    df['emp_length'] = df.emp_length.str.replace('+','')
    df['emp_length'] = df.emp_length.str.replace('< 1','0')
    df['emp_length'] = df.emp_length.str.replace('years','')
    df['emp_length'] = df.emp_length.str.replace('year','')
    df['emp_length'] = df.emp_length.str.replace('n/a','0')
    df['emp_length']= pd.to_numeric(df['emp_length'],errors='coerce')
    return df


def PreprocessingDataAndFeatureEngineering(loanDataSetFileName,loanRejectDataSetFileName):
    #Preprocessing Loan Rejects Data
    df_rej = pd.read_csv(loanRejectDataSetFileName, header=0)
    # Create a dataframe on which we will form our models and do analysis
    df_rejects = pd.DataFrame(df_rej, columns=['Amount Requested', 'Application Date', 'Loan Title', 'Risk_Score',
                                               'Debt-To-Income Ratio', 'Zip Code', 'State', 'Employment Length',
                                               'Policy Code'])
    df_rejects = renameColumns(df_rejects)
    df_rejects = HandleRejectsMissingData(df_rejects)
    miss_percent = 100 * df_rejects.isnull().sum() / len(df_rejects)
    print(miss_percent)
    df_rejects.to_csv(loanRejectDataSetFileName, header='column_names')

    #Preprocessing Loan Data
    loanDataSetdtype = {"id": object, "next_pymnt_d": object, "desc": object, "verification_status_joint": object}
    df = pd.read_csv(loanDataSetFileName, header=0,na_values = "NaN",index_col = False, dtype = loanDataSetdtype)
    # df_new = pd.DataFrame(df)
    df_copy = dropNullColumns(df)
    df_copy = getUsefuldata(df_copy)
    df_copy = HandleMissingData(df_copy)


    #Feature Engineering for Loan DataSet
    # Call to function
    #dfcopy = pd.read_csv(loanDataSetFileName, header=0)
    dfcopy = DeriveMonthYear(df_copy)
    dfcopy = DeriveNumericColumns(dfcopy)
    df_copy.to_csv(loanDataSetFileName, header='column_names')

def SummarizeRejects(loanRejectDataSetFileName):
    df_rej = pd.read_csv(loanRejectDataSetFileName, header=0)
    df_rej['year']=df_rej['Application_Date'].str.split('-',2).str[0]
    df_rej['year']=pd.to_numeric(df_rej['year'], errors='coerce')
    df_rej['month']=df_rej['Application_Date'].str.split('-',2).str[1]
    RejSummary=df_rej.groupby(['year','month']).agg({'Amount_Requested':'sum','Debt-To-Income-Ratio':'mean', 'Risk_Score':'mean'}).reset_index()
    RejSummary.to_csv("RejectSummary.csv", sep=",", index=False)

def SummaryStatsLoanData(loanDataSetFileName):
    dfcopy = pd.read_csv(loanDataSetFileName, header=0)
    summaryYear=pd.DataFrame(columns=['Year', 'Count', 'SumLoanAmount', 'AvgIntRate'])
    summaryHO=pd.DataFrame(columns=['home_ownership', 'Count', 'SumLoanAmount', 'AvgIntRate'])
    summaryGrade=pd.DataFrame(columns=['grade', 'term', 'Count', 'SumLoanAmount', 'AvgIntRate'])
    summaryStatus=pd.DataFrame(columns=['loan_status', 'Grade', 'Count', 'SumLoanAmount', 'AvgIntRate'])
    summaryState=pd.DataFrame(columns=['State', 'term', 'Count', 'SumLoanAmount', 'AvgIntRate'])

    yearCount=dfcopy.groupby(['issue_d_year']).agg({'term':'count','int_rate':'mean','loan_amnt':'sum'}).reset_index()
    summaryYear['Year']=yearCount['issue_d_year']
    summaryYear['Count']=yearCount['term']
    summaryYear['SumLoanAmount']=yearCount['loan_amnt']
    summaryYear['AvgIntRate']=yearCount['int_rate']
    summaryYear.to_csv("SummaryStatistics.csv", mode='w', index=False)

    homeOwnerCount=dfcopy.groupby(['home_ownership']).agg({'term':'count','int_rate':'mean', 'loan_amnt':'sum'}).reset_index()
    summaryHO['home_ownership']=homeOwnerCount['home_ownership']
    summaryHO['Count']=homeOwnerCount['term']
    summaryHO['SumLoanAmount']=homeOwnerCount['loan_amnt']
    summaryHO['AvgIntRate']=homeOwnerCount['int_rate']
    summaryHO.to_csv("SummaryStatistics.csv", mode='a', index=False)

    gradeTermIntRate=dfcopy.groupby(['grade', 'term']).agg({'home_ownership':'count','int_rate':'mean', 'loan_amnt':'sum'}).reset_index()
    summaryGrade['grade']=gradeTermIntRate['grade']
    summaryGrade['term']=gradeTermIntRate['term']
    summaryGrade['Count']=gradeTermIntRate['home_ownership']
    summaryGrade['SumLoanAmount']=gradeTermIntRate['loan_amnt']
    summaryGrade['AvgIntRate']=gradeTermIntRate['int_rate']
    summaryGrade.to_csv("SummaryStatistics.csv", mode='a', index=False)

    loanStatusCount=dfcopy.groupby(['loan_status','grade']).agg({'home_ownership':'count','int_rate':'mean', 'loan_amnt':'sum'}).reset_index()
    summaryStatus['loan_status']=loanStatusCount['loan_status']
    summaryStatus['Grade']=loanStatusCount['grade']
    summaryStatus['Count']=loanStatusCount['home_ownership']
    summaryStatus['SumLoanAmount']=loanStatusCount['loan_amnt']
    summaryStatus['AvgIntRate']=loanStatusCount['int_rate']
    summaryStatus.to_csv("SummaryStatistics.csv", mode='a', index=False)

    stateCount=dfcopy.groupby(['addr_state', 'term']).agg({'home_ownership':'count','loan_amnt':'sum', 'int_rate':'mean'}).reset_index()
    summaryState['State']=stateCount['addr_state']
    summaryState['term']=stateCount['term']
    summaryState['Count']=stateCount['home_ownership']
    summaryState['SumLoanAmount']=stateCount['loan_amnt']
    summaryState['AvgIntRate']=stateCount['int_rate']
    summaryState.to_csv("SummaryStatistics.csv", mode='a', index=False)