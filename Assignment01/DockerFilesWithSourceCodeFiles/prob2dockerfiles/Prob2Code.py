# coding: utf-8

import sys
import datetime as dt
import pandas as pd
import urllib.request
import zipfile, io
import csv
import os
from bs4 import BeautifulSoup
from urllib.request import urlopen
import boto.s3.connection
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import logging
from boto.exception import NoAuthHandlerFound, S3ResponseError

    
#Default Values and Constants
outputFileName = 'finalOutput.csv'
summaryFileName = 'summary.csv'

logging.basicConfig(filename='EdgarProb2Logs.txt',level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',filemode='w')
logger = logging.getLogger(__name__)



#Validate input argument and set year to 2003 if validation fails
print("*********Details***********")
print("Credentials from user")
logger.info("Credentials from user are entered")

access_key = os.getenv('Access_Key')
secret_key = os.getenv('Secret_Key')
user_bucket = os.getenv('User_Bucket')
year = os.getenv("Year")



#Validate if the user is entering credentials
if(access_key is None or access_key==""):
     print("Please enter valid access key")
     exit()
else:
     print("Access Key received")


if(secret_key is None or secret_key==""):
     print("Please enter valid secret key")
     exit()
else:
     print("Secret Key received")


if(user_bucket is None or user_bucket==""):
     print("Please enter valid Bucket for your files to upload")
     exit()
else:
     print("User Bucket:" + user_bucket)
     
if(year is None or year==""):
     print("Please enter valid year")
     exit()
else:
     print("Year is:" + str(year))



pathOutputFile = "/Docker_Case1Prob2/"+ outputFileName
pathSummaryFile = "/Docker_Case1Prob2/" + summaryFileName
#pathLogFile = "/Docker_Case1Prob2/" + logFile

logger.info("Directories for data files in Docker")
logger.info("CSV file for all 12 months with Summary"+pathOutputFile)
logger.info("CSV file for summary metrics only"+pathSummaryFile)


# Validate input argument and set year to 2003 if validation fails
try:
     if len(year) >= 2:
          #if isinstance(year,int):
          year=int(year)
          print(year)
          if not(2003 <= year <=2016):
               year=2003
     else:
          year=2003
except Exception as err:
     year=2003
print(year)
logger.info(year)



#try:
#    if len(sys.argv) >= 2:    
#        #if isinstance(sys.argv[1],int):
#        year=int(sys.argv[1])
#        print(year)
#        if not (2003 <= year <= 2016):
#            year=2003
#    else:
#        year=2003        
#except Exception as err:
#    year=2003
#print(year)


#if len(sys.argv) >= 2:    
#    if isinstance(sys.argv[1],int):
#        year=sys.argv[1]
#        if ~(year <= 2016 and year >= 2003):
#            year=2003
#    else:
#        year=2003
#else:
#    year=2003        
#print(year)


def generateURLList(year):
    urlList = []
    for m in range(1, 13):
        quarter = pd.Timestamp(dt.date(year, m, 1)).quarter
        #print(quarter)
        link='http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/'+str(year)+'/Qtr'+str(quarter)+'/log'+str(year)+format(m,'02d')+'01.zip'
        urlList.append(link)
    return urlList
logger.info("URL list is generated")


logger.info("Handling missing data")
def handleMissingData(df):
    #Remove row if ip is null
    df=df[~ df['ip'].isnull()]
    #Forward Fill or Bottom Fill date if null
    df.date.fillna(method='ffill',inplace=True)
    df.date.fillna(method='bfill',inplace=True)
    #Forward Fill or Bottom Fill time if null
    df.time.fillna(method='bfill',inplace=True)
    df.time.fillna(method='ffill',inplace=True)
    #Forward Fill zone or Bottom Fill zone if null
    df.zone.fillna(method='ffill',inplace=True)
    df.zone.fillna(method='bfill',inplace=True)
    #Concat Accession Number if extention doen't contain file name
    toConcat = df.extention.str.startswith('.',na=False)
    addedExtention = df.accession[toConcat] + df.extention[toConcat]
    normalExtention = df.extention[~toConcat]
    df.extention = addedExtention.append(normalExtention)
    #Set Code to 404 if null
    df.code.fillna(404.0, inplace=True)
    #Set File size to zero if null
    df['size'].fillna(0.0, inplace=True)
    #Set File size to zero if null
    df.idx.fillna(0.0, inplace=True)
    #Set refer to zero if null
    df.norefer.fillna(1.0, inplace=True)
    #Set agent to zero if null
    df.noagent.fillna(1.0, inplace=True)
    #Set find to zero if null
    df.find.fillna(0.0, inplace=True)
    #Set crawler to zero if null
    df.crawler.fillna(0.0, inplace=True)
    #Set browser to unknown if null
    df.browser.fillna('unknown', inplace=True)
    return df

urlList = generateURLList(int(year))
print("----Generated URLs----")
for fileURL in urlList:
     print(fileURL)
logger.info("Generated URLs for the year enterted by user %s",fileURL)    


appendWrite = True
for fileURL in urlList:
    try:
        response = urllib.request.urlopen(fileURL)
        logger.info("Check open URL")
        if response.getcode()==200:
            data = response.read()
            if zipfile.is_zipfile(io.BytesIO(data)) == True:
                z = zipfile.ZipFile(io.BytesIO(data))
                for file in z.namelist():
                    if file.endswith('.csv'):
                        if appendWrite:
                            csvFile = z.read(file)
                            df = pd.read_csv(io.BytesIO(csvFile),header=0)
                            df = handleMissingData(df)
                            df.to_csv(pathOutputFile,header ='column_names')
                            appendWrite = False
                        if not appendWrite:
                            csvFile = z.read(file)
                            df_new = pd.read_csv(io.BytesIO(csvFile),header=0)
                            df_new = handleMissingData(df_new)
                            # df.to_csv(outputFileName,header=False)
                            df_new.to_csv(pathOutputFile,mode = 'a',header=False)
                            #df.append(df_new, ignore_index=True)
                            df = pd.concat([df,df_new])
                            print("Total rows and columns in the data files tables");
                            print(df.shape)
            else:
                print("[ERROR] Invalid ZIP File found at " + fileURL)
                logger.info("[ERROR] Invalid ZIP File found at %s",fileURL)
        else:
            print("[ERROR] Invalid URL, URL( " + fileURL + " ) returned a bad HTTP response code of " + str(response.getcode()))
            logger.info("[ERROR] Invalid URL, URL( " + fileURL + " ) returned a bad HTTP response code of %s",response.getcode())
        response.close()
    except Exception as err:
        print("Error occured, possibly an interrupted Internet connection")
        logger.info("Error occured, possibly an interrupted Internet connection")


# In[169]:

#Added a column count to perform group by
df['count'] = 1


# In[230]:

logger.info("Summary metrics generated")
#browsers and count in year
df['browserRequestCount'] = 1
browser_count = df.groupby('browser').aggregate({'browserRequestCount':'sum'}).reset_index()
logger.info("Browser Request Count in a year %s",browser_count)
browser_count.to_csv(pathOutputFile, mode='a',index=False)
browser_count.to_csv(pathSummaryFile, mode='w',index=False)


# In[231]:

#Number of request in a month
df['perMonthRequestCount'] = 1
requestsByMonth = df.groupby('date').aggregate({'perMonthRequestCount':'sum'}).reset_index()
logger.info("Number of request in a month %s",requestsByMonth)
requestsByMonth.to_csv(pathOutputFile, mode='a',index=False)
requestsByMonth.to_csv(pathSummaryFile, mode='a',index=False)


# In[232]:

#For each date, total reuqests per response code
dateCodeCount = df.groupby(['date','code']).aggregate({'count':'sum'}).reset_index()
logger.info("For each date, total reuqests per response code %s",dateCodeCount)
dateCodeCount.to_csv(pathSummaryFile, mode='a',index=False)
dateCodeCount.to_csv(pathOutputFile, mode='a',index=False)


# In[233]:

#For each date, calculate the min, max and mean file sizes requested
dateCodeSize = df.groupby(['date','code']).aggregate({'size':['mean','min','max']}).reset_index()
dateCodeSize
logger.info("For each date, calculate the min, max and mean file sizes requested %s",dateCodeSize)
dateCodeSize.to_csv(pathSummaryFile, mode='a',index=False)
dateCodeSize.to_csv(pathOutputFile, mode='a',index=False)


# In[234]:

#Month, distinct IP's Count
temp_df = df.groupby(['date','ip']).aggregate({'count':'sum'}).reset_index()
temp_df['distinctIPcount'] = 1
monthDistinctIPCount = temp_df.groupby(['date']).aggregate({'distinctIPcount':'sum'}).reset_index()
logger.info("Month, distinct IP's Count %s",monthDistinctIPCount)
monthDistinctIPCount.to_csv(pathSummaryFile, mode='a',index=False)
monthDistinctIPCount.to_csv(pathOutputFile, mode='a',index=False)

#Number of crawlers that tried to scrap data every month
df_crawler = df[df.crawler == 1.0].groupby(['date','ip']).aggregate({'count':'sum'}).reset_index()
df_crawler['crawler_count'] = 1
df_crawler = df_crawler.groupby(['date']).aggregate({'crawler_count':'sum'}).reset_index()
logger.info("Number of crawlers that tried to scrap data every month: %s",df_crawler)


#Number of crawlers without 404 that tried to scrap data every month
df_crawler_without404 = df[df.code != 404.0]
df_crawler_without404 = df_crawler_without404[df_crawler_without404.crawler == 1.0].groupby(['date','ip']).aggregate({'count':'sum'}).reset_index()
df_crawler_without404['crawler_count_without404'] = 1
df_crawler_without404 = df_crawler_without404.groupby(['date']).aggregate({'crawler_count_without404':'sum'}).reset_index()
logger.info("Number of crawlers without 404 that tried to scrap data every month %s",df_crawler_without404)

df_crawler['crawler_count_without404'] = df_crawler_without404.crawler_count_without404
df_crawler.to_csv(summaryFileName, mode='a',index=False)
df_crawler.to_csv(outputFileName, mode='a',index=False)




#Now we will create and validate connection to AWS S3 based on access key and secret key user entered

try:
     aws_connection = S3Connection(access_key,secret_key)
     logger.info("Checking if the connection to Amazon S3 is successful")
     if aws_connection:
          print("Connection to Amazon S3 success!")
          logger.info("Connection success, now uploading the files to User Bucket")
          try:
               bucket = aws_connection.get_bucket(user_bucket)
               logger.info("User Bucket is %s",user_bucket)
               k = Key(bucket)
               k.key = pathOutputFile
               k.set_contents_from_filename(pathOutputFile)
               print("File uploaded to Amazon S3 in User Bucket")
               logger.info("File uploaded to Amazon S3 in User Bucket")
               y = Key(bucket)
               y.key = pathSummaryFile
               y.set_contents_from_filename(pathSummaryFile)
               z = Key(bucket)
               z.key = "LogFile" + "-"+ str(year)
               z.set_contents_from_filename('EdgarProb2Logs.txt')
               #return True
          except S3ResponseError:
               print("Connection success but Couldn't locate bucket.Check if you have given correct Bucket, Create bucket if doesn't exist.")
               logger.info("Connection success but Couldn't locate bucket.Check if you have given correct Bucket, Create bucket if doesn't exist.")
except S3ResponseError:
     print("Wrong Credentials for Amazon Keys, Check your Access and Secret keys")
     logger.info("Connection to Amazon S3 cannot be made. Wrong Credentials, Check your Access and Secret keys")
     
