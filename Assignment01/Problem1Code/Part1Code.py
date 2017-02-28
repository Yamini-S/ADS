
# coding: utf-8

# In[ ]:

import sys
import os
import pandas as pd
import shutil
import urllib.request
from bs4 import BeautifulSoup
from urllib.request import urlopen
import boto.s3.connection
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import logging
from boto.exception import NoAuthHandlerFound, S3ResponseError



# Here, we are setting up log files to log all the activities in the program
# we are logging at DEBUG level to log and print each meassage in the file
# filename is EdgarPart1Logs

logging.basicConfig(filename='EdgarPart1Logs.txt',level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',filemode='w')
logger = logging.getLogger(__name__)


# Setting up the enviornment variables
# All the following parameters are required from user
# User should follow the sequence in which these parameters are defined
access_key = os.getenv('Access_Key')
secret_key = os.getenv('Secret_Key')
user_bucket = os.getenv('User_Bucket')
cik = os.getenv('CIK')
ackNum = os.getenv('Accession_Number')



# Validation checks if the parameters are entered correctly
# Program will exit if any of the parameters are invalid.

print("*********Details***********")
print("Credentials from user")
logger.info("Credentials from user are entered")
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

if(cik is None or cik==""):
     print("Please enter a valid CIK")
     exit()
else:
     print("CIK received:" + cik)


if(ackNum is None or ackNum==""):
     print("Please enter a valid Accession Number")
     exit()
else:
     print("Accession Number received:" + ackNum)     



#Setting paths for our CSV and ZIP Files
pathToCSV ="/Docker_Case1Part1/ads"
pathToZip ="/Docker_Case1Part1/"

logger.info("Directories for data files in Docker")
logger.info("Path for CSV files set"+pathToCSV)
logger.info("Path for ZIP file set"+pathToZip)

print("---Directories for data files in Docker---")
print ("CSV Files Directory:" + pathToCSV)
print ("Zip File Directory:" + pathToZip)



# In[ ]:
#cik = sys.argv[1]


# In[ ]:

fullCIK = cik.zfill(10)

# In[ ]:

#ackNum = sys.argv[2]






# In[ ]:

def generateUrl(cik, ackNum):
    try:
        url = "https://www.sec.gov/Archives/edgar/data/" + str(cik).lstrip("0") + "/" + ackNum +"/"+ ackNum[:10] + "-"+ackNum[10:12]+"-"+ackNum[12:]+ "-index.htm"
        logger.info("Generated URL is %s",url)
        return(url)
    except:
        return 0


# In[ ]:

def checkInput(userInput):
    try:
        logger.debug("Checking user input is numeric for cik or accession number")
        val = int(userInput)
        return 1
    except ValueError:
        logger.warning("user input not correct, value shoud be numeric")
        return 0


# In[ ]:

def get_status_code(url):
    try:
        response = urlopen(url)
        code = response.getcode()
        logger.debug("Response code received %s",code)
        return code
    except:
        logger.error("connection issue")
        return 0


# In[ ]:

def TableCheck(table):
    rows = table.find_all('tr')
    for row in rows:
        #type(row)
        #print (row.find_all('td', style='vertical-align:bottom;background-color:#cceeff;padding-left:2px;padding-top:2px;padding-bottom:2px;'))
        #print (str(row).find("#cceeff"))
        if str(row).find("#cceeff") != -1:
            return True
        else:
            tds_new = row.find_all('td')
            for td_1 in tds_new:
                td_value = td_1.text.strip()
                if td_value == "$":
                    return True
                
    return False


# In[ ]:




# In[ ]:

response = urlopen('https://www.sec.gov/edgar/NYU/cik.coleft.c')


# In[ ]:

#dir = "/home/vaidehi/Downloads/ads/"+cik+ackNum


# In[ ]:

if os.path.exists(pathToCSV+'/'+cik+ackNum):
    shutil.rmtree(pathToCSV+'/'+cik+ackNum)
os.makedirs(pathToCSV+'/'+cik+ackNum)


# In[ ]:

os.chdir(pathToCSV+'/'+cik+ackNum)


# In[ ]:

if checkInput(cik) ==1 and checkInput(ackNum) == 1:
    if fullCIK in response.read().decode('utf-8', errors ='ignore'):
        urlNew = generateUrl(cik, ackNum)
        print("Fetching url:" + urlNew)
        logger.info("Fetching url %s",urlNew)
        connection = get_status_code(urlNew)
        if connection ==200:
            page = urlopen(urlNew)
            soup = BeautifulSoup(page, "lxml")
            data = []
            table=soup.find("table", class_='tableFile')
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                data.append([ele for ele in cols if ele])
            if "10-Q" or "10-K" in data[1][2]:
                stringUrl = data[1][2]
                urlPart = urlNew.rsplit('/', 1)[0]
                urlGenerated = urlPart + '/'+ stringUrl
                print ("Fetching form:" + urlGenerated)
                logger.info("Fetching form %s", urlGenerated)
                try:
                    doc = urlopen(urlGenerated)
                    #doc = urlopen("https://www.sec.gov/Archives/edgar/data/17797/000132616017000016/duk-20161231x10k.htm")
                    soup = BeautifulSoup(doc, "lxml")
                    tables=soup.find_all('table')
                    tableCount = len(tables)
                    i = 0
                    while i < tableCount:
                        tableCheck = TableCheck(tables[i])
                        if tableCheck == True:
                            dataList = []
                            rows = tables[i].find_all('tr')
                            #rowCount = len(rows)
                            for row in rows:
                                cols = row.find_all('td' )
                                cols = [ele.text.strip() for ele in cols]
                                dataList.append([ele for ele in cols if ele])
                                dataNew=[]
                                for j in dataList:
                                    dataNew.append( [ x for x in j if "$" not in x ])
                                #print(dataNew)
                                pd.DataFrame().append(dataNew).to_csv("file"+str(i)+".csv", sep = ',', encoding = 'utf-8')
                        i = i+1
                    shutil.make_archive(pathToZip+cik+ackNum, 'zip', pathToCSV+'/'+cik+ackNum)
                    logging.info("Tables downloaded at this step to Zip folder.")
                    print("Tables in form downloaded in:" + pathToZip+cik+ackNum)
                except:
                    print("Error: Invalid CIK + accession number combination; downloading default value tables. Enter correct CIK and accession number for correct files")
                    logger.error("Incorrect cik or accession number: Enter valid 10 digit CIK and valid accession number without '-'")
                    try:
                        doc = urlopen("https://www.sec.gov/Archives/edgar/data/51143/000005114313000007/ibm13q3_10q.htm")
                        soup = BeautifulSoup(doc, "lxml")
                        tables=soup.find_all('table')
                        tableCount = len(tables)
                        i = 0
                        while i < tableCount:
                            tableCheck = TableCheck(tables[i])
                            if tableCheck == True:
                                dataList = []
                                rows = tables[i].find_all('tr')
                                for row in rows:
                                    cols = row.find_all('td' )
                                    cols = [ele.text.strip() for ele in cols]
                                    dataList.append([ele for ele in cols if ele])
                                    dataNew=[]
                                    for j in dataList:
                                        dataNew.append( [ x for x in j if "$" not in x ])

                                    pd.DataFrame().append(dataNew).to_csv("file"+str(i)+".csv", sep = ',', encoding = 'utf-8')
                            i = i+1
                        shutil.make_archive(pathToZip+cik+ackNum, 'zip', pathToCSV+'/'+cik+ackNum)
                        print("Tables in form downloaded in folder:" +pathToZip+cik+ackNum)
                        logging.info("Tables downloaded at this step to Zip folder.")
                    except:
                        print ("Poor Network Connection")
                        logging.warning("Poor Network Connection")

            else:
                print ("Form not available; downloading default value tables")
                logging.warning("Form not available; downloading default value tables")
                try:
                    doc = urlopen("https://www.sec.gov/Archives/edgar/data/51143/000005114313000007/ibm13q3_10q.htm")
                    soup = BeautifulSoup(doc, "lxml")
                    tables=soup.find_all('table')
                    tableCount = len(tables)
                    i = 0
                    while i < tableCount:
                        tableCheck = TableCheck(tables[i])
                        if tableCheck == True:
                            dataList = []
                            rows = tables[i].find_all('tr')
                            for row in rows:
                                cols = row.find_all('td' )
                                cols = [ele.text.strip() for ele in cols]
                                dataList.append([ele for ele in cols if ele])
                                dataNew=[]
                                for j in dataList:
                                    dataNew.append( [ x for x in j if "$" not in x ])

                                pd.DataFrame().append(dataNew).to_csv("file"+str(i)+".csv", sep = ',', encoding = 'utf-8')
                        i = i+1
                    shutil.make_archive(pathToZip+cik+ackNum, 'zip', pathToCSV+'/'+cik+ackNum)
                    print("Tables in form downloaded in folder:" +pathToZip+cik+ackNum)
                    logging.info("Tables downloaded at this step to Zip folder.")
                except:
                    print ("Poor Network Connection")
                    logging.warning("Poor Network Connection")


        else:
            print ("ERROR: Incorrect cik or accession number: Enter valid 10 digit CIK and valid accession number without '-'; downloading default value tables")
            logging.error("ERROR: Incorrect cik or accession number: Enter valid 10 digit CIK and valid accession number without '-'; downloading default value tables")
            try:
                doc = urlopen("https://www.sec.gov/Archives/edgar/data/51143/000005114313000007/ibm13q3_10q.htm")
                soup = BeautifulSoup(doc, "lxml")
                tables=soup.find_all('table')
                tableCount = len(tables)
                i = 0
                while i < tableCount:
                    tableCheck = TableCheck(tables[i])
                    if tableCheck == True:
                        dataList = []
                        rows = tables[i].find_all('tr')
                        for row in rows:
                            cols = row.find_all('td' )
                            cols = [ele.text.strip() for ele in cols]
                            dataList.append([ele for ele in cols if ele])
                            dataNew=[]
                            for j in dataList:
                                dataNew.append( [ x for x in j if "$" not in x ])

                            pd.DataFrame().append(dataNew).to_csv("file"+str(i)+".csv", sep = ',', encoding = 'utf-8')
                    i = i+1
                shutil.make_archive(pathToZip+cik+ackNum, 'zip', pathToCSV+'/'+cik+ackNum)
                print("Tables in form downloaded in folder:" + pathToZip+cik+ackNum)
                logging.info("Tables downloaded at this step to Zip folder.")
            except:
                print ("Poor Network Connection")
                logging.warning("Poor Network Connection")


    else:
        print("Invalid CIK; downloading default value tables. Enter correct CIK and accession number for correct files.")
        try:
            doc = urlopen("https://www.sec.gov/Archives/edgar/data/51143/000005114313000007/ibm13q3_10q.htm")
            soup = BeautifulSoup(doc, "lxml")
            tables=soup.find_all('table')
            tableCount = len(tables)
            i = 0
            while i < tableCount:
                tableCheck = TableCheck(tables[i])
                if tableCheck == True:
                    dataList = []
                    rows = tables[i].find_all('tr')
                    for row in rows:
                        cols = row.find_all('td' )
                        cols = [ele.text.strip() for ele in cols]
                        dataList.append([ele for ele in cols if ele])
                        dataNew=[]
                        for j in dataList:
                            dataNew.append( [ x for x in j if "$" not in x ])

                        pd.DataFrame().append(dataNew).to_csv("file"+str(i)+".csv", sep = ',', encoding = 'utf-8')
                i = i+1
            shutil.make_archive(pathToZip+cik+ackNum, 'zip', pathToCSV+'/'+cik+ackNum)
            print("Tables in form downloaded in folder:" + pathToZip+cik+ackNum)
            logging.info("Tables downloaded at this step to Zip folder.")
        except:
            print ("Poor Network Connection")
            logging.warning("Poor Network Connection")



else:
    print("Enter numerical value for CIK and accession number; downloading default value tables. Enter correct CIK and accession number for correct files.")
    logger.info("Enter numerical value for CIK and accession number; downloading default value tables. Enter correct CIK and accession number for correct files.")
    try:
        doc = urlopen("https://www.sec.gov/Archives/edgar/data/51143/000005114313000007/ibm13q3_10q.htm")
        soup = BeautifulSoup(doc, "lxml")
        tables=soup.find_all('table')
        tableCount = len(tables)
        i = 0
        while i < tableCount:
            tableCheck = TableCheck(tables[i])
            if tableCheck == True:
                dataList = []
                rows = tables[i].find_all('tr')
                for row in rows:
                    cols = row.find_all('td' )
                    cols = [ele.text.strip() for ele in cols]
                    dataList.append([ele for ele in cols if ele])
                    dataNew=[]
                    for j in dataList:
                        dataNew.append( [ x for x in j if "$" not in x ])

                    pd.DataFrame().append(dataNew).to_csv("file"+str(i)+".csv", sep = ',', encoding = 'utf-8')
            i = i+1
        shutil.make_archive(pathToZip+cik+ackNum, 'zip', pathToCSV+'/'+cik+ackNum)
        print("Tables in form downloaded in folder:" + pathToZip+cik+ackNum)
        logging.info("Tables downloaded at this step to Zip folder.")
    except:
        print ("Poor Network Connection")
        logging.warning("Poor Network Connection")


# In[ ]:

#Now we will make connection to AWS S3 based on access key and secret key user entered

try:
     aws_connection = S3Connection(access_key,secret_key)
     logger.info("Checking if the connection to Amazon S3 is successful")
     if aws_connection:
          print("Connection to Amazon S3 success!")
          logger.info("Connection success, now uploading the files to User Bucket")
          try:
              bucket = aws_connection.get_bucket(user_bucket, validate=False)
              logger.info("User Bucket is %s",user_bucket)
              k = Key(bucket)
              k.key = cik+ackNum
              k.set_contents_from_filename(pathToZip+cik+ackNum+'.zip')
              print("File uploaded to Amazon S3 in User Bucket")
              logger.info("File uploaded to Amazon S3 in User Bucket")
              y = Key(bucket)
              y.key = "LogFile" + "-"+ cik+ackNum
              y.set_contents_from_filename(pathToZip+"EdgarPart1Logs.txt")
              #return true
          except S3ResponseError:
               print("Connection success but Couldn't locate bucket.Check if you have given correct Bucket, Create bucket if doesn't exist.")
               logger.info("Connection success but Couldn't locate bucket.Check if you have given correct Bucket, Create bucket if doesn't exist.")
except S3ResponseError:
     print("Wrong Credentials for Amazon Keys, Check your Access and Secret keys")
     logger.error("Connection to Amazon S3 cannot be made. Wrong Credentials, Check your Access and Secret keys")
