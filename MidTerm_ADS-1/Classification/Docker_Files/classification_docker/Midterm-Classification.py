
# coding: utf-8

# # Loan Performance Data Set - Classification of Loan Delinquency Status

import sys
import datetime as dt
import pandas as pd
import urllib.request
import zipfile, io
import csv
import numpy as np
import math
import matplotlib.pyplot as plt
from matplotlib import style
import seaborn as sns
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn import neighbors
from sklearn import svm
from sklearn import metrics
from sklearn import neural_network
from sklearn.feature_selection import RFE
from sklearn import preprocessing
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.feature_selection import f_classif


# Preprocessing of Data

def preprocess(df_input):
    #Remove Spaces and Unknown
    df_input['CURRENT_LOAN_DELINQUENCY_STATUS'] = df_input['CURRENT_LOAN_DELINQUENCY_STATUS'].replace(['XX','   '],np.nan)
    df_input['REPURCHASE_FLAG'] = df_input['REPURCHASE_FLAG'].replace(['  '],'NA')
    df_input['MODIFICATION_FLAG'] = df_input['MODIFICATION_FLAG'].replace(['  '],'not_modfified')
    df_input['ZERO_BALANCE_CODE'] = df_input['ZERO_BALANCE_CODE'].replace(['  '],'NA')
    df_input['ZERO_BALANCE_EFFECTIVE_DATE'] = df_input['ZERO_BALANCE_EFFECTIVE_DATE'].replace(['      '],'NA')
    return df_input


def handleMissingData(df_input):
    #If null LOAN_SEQUENCE_NUMBER replace with NA
    df_input['LOAN_SEQUENCE_NUMBER'].fillna('NA',inplace=True)
    #If null replace CURRENT_ACTUAL_UPB with mean
    #df_input['CURRENT_ACTUAL_UPB'].fillna(0,inplace=True)
    #df_input['CURRENT_ACTUAL_UPB'].replace(0,df.Credit_Score.mean(),inplace=True)
    #If null Forward Fill or Bottom MONTHLY_REPORTING_PERIOD if null
    df_input.MONTHLY_REPORTING_PERIOD.fillna(method='ffill',inplace=True)
    df_input.MONTHLY_REPORTING_PERIOD.fillna(method='bfill',inplace=True)
    #Drop Rows of CURRENT_LOAN_DELINQUENCY_STATUS if Null
    df_input=df_input[~ df_input['CURRENT_LOAN_DELINQUENCY_STATUS'].isnull()]
    #Interpolate LOAN_AGE, REMAINING_MONTHS_TO_LEGAL_MATURITY
    df_input['LOAN_AGE'] = df_input['LOAN_AGE'].interpolate()
    df_input['REMAINING_MONTHS_TO_LEGAL_MATURITY'] = df_input['REMAINING_MONTHS_TO_LEGAL_MATURITY'].interpolate()
    #REPURCHASE_FLAG if null fill unknown
    df_input['REPURCHASE_FLAG'].fillna('unknown',inplace=True)
    #MODIFICATION_FLAG if null fill unknown
    df_input['MODIFICATION_FLAG'].fillna('unknown',inplace=True)
    #ZERO_BALANCE_CODE if null fill unknown
    df_input['ZERO_BALANCE_CODE'].fillna('unknown',inplace=True)
    #ZERO_BALANCE_EFFECTIVE_DATE if null fill NA    
    df_input['ZERO_BALANCE_EFFECTIVE_DATE'].fillna('NA',inplace=True)
    #CURRENT_INTEREST_RATE interpolate
    df_input['CURRENT_INTEREST_RATE'] = df_input['CURRENT_INTEREST_RATE'].interpolate()    
    #MI_RECOVERIES
    df_input['MI_RECOVERIES'].fillna(0,inplace=True)
    #NET_SALES_PROCEEDS 
    df_input['NET_SALES_PROCEEDS'].fillna(0,inplace=True)
    #NON_MI_RECOVERIES
    df_input['NON_MI_RECOVERIES'].fillna(0,inplace=True)
    #EXPENSES                      
    df_input['EXPENSES'].fillna(0,inplace=True)
    #Legal_Costs                   
    df_input['Legal_Costs'].fillna(0,inplace=True)
    #Maintenance_and_Preservation_Costs
    df_input['Maintenance_and_Preservation_Costs'].fillna(0,inplace=True)
    #Taxes_and_Insurance
    df_input['Taxes_and_Insurance'].fillna(0,inplace=True)
    #Miscellaneous_Expenses
    df_input['Miscellaneous_Expenses'].fillna(0,inplace=True)
    #Actual_Loss_Calculation
    df_input['Actual_Loss_Calculation'].fillna(0,inplace=True)
    #Modification_Cost
    df_input['Modification_Cost'].fillna(0,inplace=True)
    return df_input


def addNewColumnDelinquentorNonDelinquent(df):
    df['NEW_LOAN_DELINQUENCY_STATUS'] = pd.to_numeric(df.CURRENT_LOAN_DELINQUENCY_STATUS,errors = 'coerce')
    df['NEW_LOAN_DELINQUENCY_STATUS'].fillna(1, inplace = True)
    df_notZeros = df.NEW_LOAN_DELINQUENCY_STATUS[df['NEW_LOAN_DELINQUENCY_STATUS'] != 0]
    df_notZeros = df_notZeros/df_notZeros
    print(df_notZeros.shape)
    df_Zeros = df.NEW_LOAN_DELINQUENCY_STATUS[df['NEW_LOAN_DELINQUENCY_STATUS'] == 0]
    print(df_Zeros.shape)
    df_y = df_notZeros.append(df_Zeros)
    df['NEW_LOAN_DELINQUENCY_STATUS'] = df_y
    return df


def checkPercentageOfMissingData(df):
    mis_val_percent = 100 * df.isnull().sum()/len(df)
    print(mis_val_percent)



def normalizeColumn(df_col):
    return preprocessing.normalize(df_col)


def createDFfromURL(fileURL):
    response = urllib.request.urlopen(fileURL)
    df = pd.DataFrame()
    if response.getcode()==200:
        data = response.read()
        if zipfile.is_zipfile(io.BytesIO(data)) == True:
            #print("Valid Zip file")
            z = zipfile.ZipFile(io.BytesIO(data))
            for file in z.namelist():
                #print(file)
                if file.find('_time_') != -1:
                    print("inside: "+file)
                    #print('Inside IF True')
                    csvFile = z.read(file)
                    df = pd.read_csv(io.BytesIO(csvFile),sep="|",header=None)
    print(df.shape)
    df.columns = ['LOAN_SEQUENCE_NUMBER','MONTHLY_REPORTING_PERIOD','CURRENT_ACTUAL_UPB','CURRENT_LOAN_DELINQUENCY_STATUS','LOAN_AGE','REMAINING_MONTHS_TO_LEGAL_MATURITY','REPURCHASE_FLAG','MODIFICATION_FLAG','ZERO_BALANCE_CODE','ZERO_BALANCE_EFFECTIVE_DATE','CURRENT_INTEREST_RATE','CURRENT_DEFERRED_UPB','DUE_DATE_OF_LAST_PAID_INSTALLMENT','MI_RECOVERIES','NET_SALES_PROCEEDS','NON_MI_RECOVERIES','EXPENSES','Legal_Costs','Maintenance_and_Preservation_Costs','Taxes_and_Insurance','Miscellaneous_Expenses','Actual_Loss_Calculation','Modification_Cost','unknown']
    return df


def filterFrameWithRequiredFeatuers(df_input,df_input_test, ranking):
    df_new_train_x = pd.DataFrame()
    df_new_test_x = pd.DataFrame()
    print(df_new_test_x.head())
    for i in ranking_out:
        score,col_name  = i
        if score == 1:
            df_new_train_x[col_name] = df_input[col_name]
            df_new_test_x[col_name] = df_input_test[col_name]
    checkPercentageOfMissingData(df_new_train_x)
    checkPercentageOfMissingData(df_new_test_x)
    return (df_new_train_x,df_new_test_x)


def computeBestModel(list_of_model_and_accurancySocres):
    best_algo = None
    best_algo_accu_score = 0
    best_algo_model = None
    for value in list_of_model_and_accurancySocres:
        if best_algo != None:
            if value[1] > best_algo_accu_score:
                best_algo = value[0]
                best_algo_accu_score = value[1]
                best_algo_model = value[2] 
        else:
            best_algo = value[0]
            best_algo_accu_score = value[1]
            best_algo_model = value[2]
    print(best_algo + ' is best model with Accuracy Score of: ' +str(best_algo_accu_score))
    return best_algo


def logisticRegression(x_train, y_train):
    print('Logistic Regression model computation has started')
    model = LogisticRegression()
    model.fit(x_train,y_train)
    print('Logistic Regression model computation has completed')    
    return (model, 'Logistic Regression')

def predictAndEvaluate(model,x_test,y_test):
    # predict class labels for the test set
    print('Model testing has started')
    predicted = model.predict(x_test)
    probs = model.predict_proba(x_test)[:, 1]
    print('Model testing has completed')
    # generate evaluation metrics
    accu_score = metrics.accuracy_score(y_test, predicted)
    print('Accuracy Score: ' + str(accu_score))
    cm = metrics.confusion_matrix(y_test, predicted)
    print('Confusion Matrix:')
    print(cm)
    y_test = y_test.astype(np.float)
    fpr, tpr, _ = metrics.roc_curve(y_test, probs)
    '''
    #Plot ROC curve
    get_ipython().magic(u'matplotlib inline')
    plt.figure()
    plt.plot(fpr, tpr, label='ROC curve')
    plt.plot([0, 1], [0, 1], 'k--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('1-Specificity')
    plt.ylabel('Sensitivity')
    plt.title('ROC curve')
    plt.legend(loc="lower right")
    plt.show()
    '''
    return (accu_score,cm)

def randomForestClassifier(x_train, y_train):
    print('Random Forest model computation has started')    
    model = RandomForestClassifier(n_jobs=2)
    model = model.fit(x_train,y_train)
    print('Random Forest model computation has completed')    
    return (model, 'Random Forest Classifier')

def neuralNetClassifier(x_train, y_train):
    print('Neural Network based Classification model computation has started')     
    model = neural_network.MLPClassifier(solver='lbfgs', alpha=1e-5, hidden_layer_sizes=(5, 2), random_state=1)
    model = model.fit(x_train, y_train)
    print('Neural Network based Classification model computation has completed')     
    return (model,'Neural Network Classifier')

def svc(x_train, y_train):
    print('Support Vector Classification model computation has started')    
    model = svm.SVC(probability=True)
    model = model.fit(x_train, y_train) 
    print('Support Vector Classification model computation has completed')     
    return (model,'Support Vector Classification')

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
    return (urlTrainList,urlTestList)

quarter_final = 0
year_final = 0
while True:
    try:
        print('Enter Quarter Number:')
        quarter_input = input()
        quarter_final = int(quarter_input)
        if quarter_final >= 1 and quarter_final <= 4:
            break;
        else:
            print('Quarter Number should be between 1 and 4.')
    except Exception as e:
        print("Invalid Quarter Number Entered. Try Again.")
while True:
    try:
        print('Enter Year Number:')
        year_final = input()
        year_final = int(year_final)
        if year_final >= 1999 and year_final <= 2016:
            break;
        else:
            print('Year should be between 1999 and 2016.')
    except Exception as e:
        print("Invalid Year Entered. Try Again.")
    


print('Starting Data Download From below URLse')
urlTrainList,urlTestList = generateURLList(quarter_final,year_final)
print(urlTrainList[0])
print(urlTestList[0])

df = createDFfromURL(urlTrainList[0])
df_test = createDFfromURL(urlTestList[0])

#Preprocess Data, Handle Missing, Add NEW_LOAN_DELINQUENCY_STATUS 
df = preprocess(df)
df = handleMissingData(df)
df = addNewColumnDelinquentorNonDelinquent(df)

#Preprocess Data, Handle Missing, Add NEW_LOAN_DELINQUENCY_STATUS 
df_test = preprocess(df_test)
df_test = handleMissingData(df_test)
df_test = addNewColumnDelinquentorNonDelinquent(df_test)

print('Missing Data percentage in by each column')

checkPercentageOfMissingData(df)
checkPercentageOfMissingData(df_test)

#Factorize Data
df['REPURCHASE_FLAG_FACTORIZE'] = pd.factorize(df['REPURCHASE_FLAG'])[0]
df['MODIFICATION_FLAG_FACTORIZE'] = pd.factorize(df['MODIFICATION_FLAG'])[0]
df['ZERO_BALANCE_CODE_FACTORIZE'] = pd.factorize(df['ZERO_BALANCE_CODE'])[0]
df_test['REPURCHASE_FLAG_FACTORIZE'] = pd.factorize(df_test['REPURCHASE_FLAG'])[0]
df_test['MODIFICATION_FLAG_FACTORIZE'] = pd.factorize(df_test['MODIFICATION_FLAG'])[0]
df_test['ZERO_BALANCE_CODE_FACTORIZE'] = pd.factorize(df_test['ZERO_BALANCE_CODE'])[0]

print(df.shape)
print(df_test.shape)

x_train = pd.DataFrame()
x_train = df[['CURRENT_ACTUAL_UPB','LOAN_AGE','REMAINING_MONTHS_TO_LEGAL_MATURITY','CURRENT_INTEREST_RATE','CURRENT_DEFERRED_UPB','MI_RECOVERIES','NON_MI_RECOVERIES','Actual_Loss_Calculation','Modification_Cost','REPURCHASE_FLAG_FACTORIZE','MODIFICATION_FLAG_FACTORIZE','ZERO_BALANCE_CODE_FACTORIZE','EXPENSES']]
y_train = pd.DataFrame()
y_train['NEW_LOAN_DELINQUENCY_STATUS'] = df['NEW_LOAN_DELINQUENCY_STATUS']
y_train = np.ravel(y_train)
x_train.dtypes
x_test = pd.DataFrame()
x_test = df_test[['CURRENT_ACTUAL_UPB','LOAN_AGE','REMAINING_MONTHS_TO_LEGAL_MATURITY','CURRENT_INTEREST_RATE','CURRENT_DEFERRED_UPB','MI_RECOVERIES','NON_MI_RECOVERIES','Actual_Loss_Calculation','Modification_Cost','REPURCHASE_FLAG_FACTORIZE','MODIFICATION_FLAG_FACTORIZE','ZERO_BALANCE_CODE_FACTORIZE','EXPENSES']]
y_test = pd.DataFrame()
y_test['NEW_LOAN_DELINQUENCY_STATUS'] = df_test['NEW_LOAN_DELINQUENCY_STATUS']
y_test = np.ravel(y_test)
x_test.dtypes

print(x_train.shape)
print(x_test.shape)

model, algo_name = logisticRegression(x_train, y_train)
accu_score,confusion_matrix = predictAndEvaluate(model, x_test, y_test)

model, algo_name = randomForestClassifier(x_train, y_train)
accu_score,confusion_matrix = predictAndEvaluate(model, x_test, y_test)

model, algo_name = neuralNetClassifier(x_train, y_train)
accu_score, confusion_matrix = predictAndEvaluate(model, x_test, y_test)

list_of_model_and_accurancySocres = []

print('RFE based Feature Elimination will be started from next step and 5 features will be selected from thr total number of featuers')

# Create the RFE object and rank each pixel
lr = LogisticRegression()
rfe = RFE(estimator=lr, n_features_to_select=5, step=2)
rfe.fit(x_train.head(1000000),y_train[0:1000000])
ranking = rfe.ranking_
ranking_out = zip(map(lambda x: round(x, 4), rfe.ranking_), x_train.columns)
print(sorted(zip(map(lambda x: round(x, 4), rfe.ranking_), x_train.columns)))
df_train_x,df_test_x = filterFrameWithRequiredFeatuers(x_train,x_test, ranking_out)
print(df_train_x.shape)
print(df_test_x.shape)

counts,counts_1 = np.unique(y_test,return_counts=True)

col_names = ['Classification_Algorithm','Number_of_Actual_Delinquents','Number_of_predicted_delinquents','Number_of_records_in_dataset','Number_of_delinquents_properly_classified','Number_of_non- delinquents_improperly_classified_as_delinquents']
out_matrix = pd.DataFrame(columns=col_names)


def addRowToDataFrame(confusion_matrix_input,y_values,col_names,output_matrix,algo_name_in):
    counts,counts_1 = np.unique(y_values,return_counts=True)
    temp = [counts_1[1],confusion_matrix_input[0][1]+confusion_matrix_input[1][1],len(y_values),confusion_matrix_input[1][1],confusion_matrix_input[0][1]]
    temp = np.asarray(temp)
    print(temp)
    print(temp[0])
    output_matrix = output_matrix.append({'Classification_Algorithm':algo_name_in,'Number_of_Actual_Delinquents':temp[0], 'Number_of_predicted_delinquents':temp[1],'Number_of_records_in_dataset':temp[2],'Number_of_delinquents_properly_classified':temp[3],'Number_of_non- delinquents_improperly_classified_as_delinquents':temp[4]}, ignore_index=True)
    return output_matrix

model,algo_name = logisticRegression(df_train_x,y_train)
accu_score, confusion_matrix = predictAndEvaluate(model, df_test_x, y_test)
list_of_model_and_accurancySocres.append((algo_name,accu_score, model))
out_matrix = addRowToDataFrame(confusion_matrix,y_test,col_names,out_matrix,algo_name)

model, algo_name = randomForestClassifier(df_train_x, y_train)
accu_score, confusion_matrix = predictAndEvaluate(model, df_test_x, y_test)
list_of_model_and_accurancySocres.append((algo_name,accu_score, model))
out_matrix = addRowToDataFrame(confusion_matrix,y_test,col_names,out_matrix,algo_name)

model, algo_name = neuralNetClassifier(df_train_x, y_train)
accu_score, confusion_matrix = predictAndEvaluate(model, df_test_x, y_test)
list_of_model_and_accurancySocres.append((algo_name,accu_score, model))
out_matrix = addRowToDataFrame(confusion_matrix,y_test,col_names,out_matrix,algo_name)

out_matrix



best_algo_picked = computeBestModel(list_of_model_and_accurancySocres)


