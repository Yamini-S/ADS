import luigi
from luigi.contrib.s3 import S3Client
from luigi.contrib.s3 import S3Target
from luigi.contrib.s3 import S3PathTask
from luigi.local_target import LocalTarget
from luigi import configuration
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import Loan_Download
import pandas as pd

class DownLoadLoanDataSetTask(luigi.Task):

    awsaccesskeyid = luigi.Parameter()
    awssecretaccesskey = luigi.Parameter()
    loanDataFile = luigi.Parameter(default='loan.csv')
    rejectLoanDataFile = luigi.Parameter(default='rejectLoan.csv')
    bucketName = luigi.Parameter()
    taskFilepath = luigi.Parameter('DownLoadLoanDataSetTask.txt')

    def run(self):
        # Logic For Download Data goes here
        print('=========================')
        print('Downloading Loan Data Set to Local')
        print('=========================')
        Loan_Download.downloadLoanDataSet(self.loanDataFile)
        self.fileHandle = open(self.taskFilepath, 'w')
        self.fileHandle.close()
        print('=========================')
        print('Completed Downloading Loan Data Set to Local')
        print('=========================')
        self.uploadFileToS3()

    def output(self):
        return S3TargetExists(self.awsaccesskeyid, self.awssecretaccesskey, self.bucketName, self.taskFilepath)

    def uploadFileToS3(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.awsaccesskeyid,
            aws_secret_access_key=self.awssecretaccesskey,
            config=Config(signature_version='s3v4')
        )
        if client:
            print('========================')
            print('Uploading Reject Loan Data File to S3 bucket')
            print('========================')
            client.upload_file(self.taskFilepath, self.bucketName, self.taskFilepath)

class DownloadRejectLoanDataSetTask(luigi.Task):

    awsaccesskeyid = luigi.Parameter()
    awssecretaccesskey = luigi.Parameter()
    loanDataFile = luigi.Parameter(default='loan.csv')
    rejectLoanDataFile = luigi.Parameter(default='rejectLoan.csv')
    bucketName = luigi.Parameter()
    taskFilepath = luigi.Parameter('DownloadRejectLoanDataSetTask.txt')

    def run(self):
        # Logic For Download Data goes here
        print('=========================')
        print('Downloading Reject Loan Data Set to Local')
        print('=========================')
        Loan_Download.downloadRejectLoanDataSet(self.rejectLoanDataFile)
        self.fileHandle = open(self.taskFilepath, 'w')
        self.fileHandle.close()
        print('=========================')
        print('Completed Downloading Reject Loan Data Set to Local')
        print('=========================')
        self.uploadFileToS3()

    def output(self):
        return S3TargetExists(self.awsaccesskeyid, self.awssecretaccesskey, self.bucketName, self.taskFilepath)

    def uploadFileToS3(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.awsaccesskeyid,
            aws_secret_access_key=self.awssecretaccesskey,
            config=Config(signature_version='s3v4')
        )
        if client:
            print('========================')
            print('Uploading Reject Loan Data File to S3 bucket')
            print('========================')
            client.upload_file(self.taskFilepath, self.bucketName, self.taskFilepath)

class PreprocessingBothDataSetsAndFeatureEngineering(luigi.Task):
    awsaccesskeyid = luigi.Parameter()
    awssecretaccesskey = luigi.Parameter()
    loanDataFile = luigi.Parameter(default='loan.csv')
    rejectLoanDataFile = luigi.Parameter(default='rejectLoan.csv')
    bucketName = luigi.Parameter()
    taskFilepath = luigi.Parameter('PreprocessingBothDataSetsAndFeatureEngineering.txt')

    def requires(self):
        return [DownLoadLoanDataSetTask(self.awsaccesskeyid, self.awssecretaccesskey, self.loanDataFile,
                                        self.rejectLoanDataFile, self.bucketName),DownloadRejectLoanDataSetTask(self.awsaccesskeyid, self.awssecretaccesskey, self.loanDataFile,
                                        self.rejectLoanDataFile, self.bucketName)]

    def run(self):
        # Logic For Preprocess Data goes here
        print('=========================')
        print('Started Preprocessing & Feature Engineering of Loan & Reject Loan Data Sets')
        print('=========================')
        Loan_Download.PreprocessingDataAndFeatureEngineering(self.loanDataFile,self.rejectLoanDataFile)
        self.fileHandle = open(self.taskFilepath, 'w')
        self.fileHandle.close()
        print('=========================')
        print('Completed Preprocessing & Feature Engineering of Loan & Reject Loan Data Sets')
        print('=========================')
        self.uploadFileToS3()

    def output(self):
        return S3TargetExists(self.awsaccesskeyid, self.awssecretaccesskey, self.bucketName, self.taskFilepath)

    def uploadFileToS3(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.awsaccesskeyid,
            aws_secret_access_key=self.awssecretaccesskey,
            config=Config(signature_version='s3v4')
        )
        if client:
            print('========================')
            print('Uploading Reject Loan Data File to S3 bucket')
            print('========================')
            client.upload_file(self.taskFilepath, self.bucketName, self.taskFilepath)

class SummarizationTask(luigi.Task):
    awsaccesskeyid = luigi.Parameter()
    awssecretaccesskey = luigi.Parameter()
    loanDataFile = luigi.Parameter(default='loan.csv')
    rejectLoanDataFile = luigi.Parameter(default='rejectLoan.csv')
    bucketName = luigi.Parameter()
    taskFilepath = luigi.Parameter('SummarizationTask.txt')

    def requires(self):
        return [PreprocessingBothDataSetsAndFeatureEngineering(self.awsaccesskeyid, self.awssecretaccesskey,
                                                               self.loanDataFile,
                                                               self.rejectLoanDataFile, self.bucketName)]

    def run(self):
        # Logic For Summarization goes here
        print('=========================')
        print('Summarization has started!')
        print('=========================')
        Loan_Download.SummarizeRejects(self.rejectLoanDataFile)
        Loan_Download.SummaryStatsLoanData(self.loanDataFile)
        self.fileHandle = open(self.taskFilepath, 'w')
        self.fileHandle.close()
        print('=========================')
        print('Summarization has completed!')
        print('=========================')
        self.uploadFileToS3()

    def output(self):
        # return LocalTarget('SummarizationTask.txt')
        return S3TargetExists(self.awsaccesskeyid, self.awssecretaccesskey, self.bucketName, self.taskFilepath)

    def uploadFileToS3(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.awsaccesskeyid,
            aws_secret_access_key=self.awssecretaccesskey,
            config=Config(signature_version='s3v4')
        )
        if client:
            print('========================')
            print('Uploading Reject Loan Data File to S3 bucket')
            print('========================')
            client.upload_file(self.taskFilepath, self.bucketName, self.taskFilepath)

class UploadLoanDataToS3(luigi.Task):
    awsaccesskeyid = luigi.Parameter()
    awssecretaccesskey = luigi.Parameter()
    loanDataFile = luigi.Parameter(default='loan.csv')
    rejectLoanDataFile = luigi.Parameter(default='rejectLoan.csv')
    bucketName = luigi.Parameter()

    def requires(self):
        return [SummarizationTask(self.awsaccesskeyid, self.awssecretaccesskey, self.loanDataFile,
                                         self.rejectLoanDataFile, self.bucketName)]

    def run(self):
        # Logic For Upload Loan Data to S3 goes here
        self.uploadFileToS3()
        print('=========================')
        print('Completed Uploading Loan Data File to S3 bucket')
        print('=========================')

    def output(self):
        return S3TargetExists(self.awsaccesskeyid, self.awssecretaccesskey, self.bucketName, self.loanDataFile)

    def uploadFileToS3(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.awsaccesskeyid,
            aws_secret_access_key=self.awssecretaccesskey,
            config=Config(signature_version='s3v4')
        )
        if client:
            print('========================')
            print('Uploading Loan Data File to S3 bucket')
            print('========================')
            client.upload_file(self.loanDataFile, self.bucketName , self.loanDataFile)

class UploadRejectLoanDataToS3(luigi.Task):
    awsaccesskeyid = luigi.Parameter()
    awssecretaccesskey = luigi.Parameter()
    loanDataFile = luigi.Parameter(default='loan.csv')
    rejectLoanDataFile = luigi.Parameter(default='rejectLoan.csv')
    bucketName = luigi.Parameter()

    def requires(self):
        return [UploadLoanDataToS3(self.awsaccesskeyid, self.awssecretaccesskey, self.loanDataFile,
                                        self.rejectLoanDataFile, self.bucketName)]

    def run(self):
        # Logic For Summarization goes here
        self.uploadFileToS3()
        print('========================')
        print('Completed Uploading Reject Loan Data File to S3 bucket')
        print('========================')

    def output(self):
        return S3TargetExists(self.awsaccesskeyid, self.awssecretaccesskey, self.bucketName, self.rejectLoanDataFile)

    def uploadFileToS3(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.awsaccesskeyid,
            aws_secret_access_key=self.awssecretaccesskey,
            config=Config(signature_version='s3v4')
        )
        if client:
            print('========================')
            print('Uploading Reject Loan Data File to S3 bucket')
            print('========================')
            client.upload_file(self.rejectLoanDataFile, self.bucketName, self.rejectLoanDataFile)

class UploadSummaryFilesToS3(luigi.Task):
    awsaccesskeyid = luigi.Parameter()
    awssecretaccesskey = luigi.Parameter()
    loanDataFile = luigi.Parameter(default='loan.csv')
    rejectLoanDataFile = luigi.Parameter(default='rejectLoan.csv')
    bucketName = luigi.Parameter()
    loanSummaryFileName = luigi.Parameter(default='SummaryStatistics.csv')
    rejectLoanSummaryFileName = luigi.Parameter(default='RejectSummary.csv')

    def requires(self):
        return [UploadRejectLoanDataToS3(self.awsaccesskeyid, self.awssecretaccesskey, self.loanDataFile,
                                        self.rejectLoanDataFile, self.bucketName)]

    def run(self):
        # Logic For Summarization goes here
        self.uploadFileToS3()
        print('========================')
        print('Completed Uploading Summary Files to S3 bucket')
        print('========================')

    def output(self):
        return S3TargetExists(self.awsaccesskeyid, self.awssecretaccesskey, self.bucketName, self.loanSummaryFileName)

    def uploadFileToS3(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.awsaccesskeyid,
            aws_secret_access_key=self.awssecretaccesskey,
            config=Config(signature_version='s3v4')
        )
        if client:
            print('========================')
            print('Uploading Summary Files to S3 bucket')
            print('========================')
            client.upload_file(self.loanSummaryFileName, self.bucketName, self.loanSummaryFileName)
            client.upload_file(self.rejectLoanSummaryFileName, self.bucketName, self.rejectLoanSummaryFileName)

class S3TargetExists(luigi.Target):

    def __init__(self, aws_access_key_id, aws_secret_access_key, bucketName, fileName):
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_access_key_id = aws_access_key_id
        self.bucketName = bucketName
        self.fileName = fileName


    def exists(self):
        client = CreateBotoClient(self.aws_access_key_id,self.aws_secret_access_key).createClient()
        if client:
            try:
                print('===========================')
                print('Checking if bucket exists')
                print('===========================')
                client.head_bucket(Bucket=self.bucketName)
                print('===========================')
                print('Bucket found!')
                print('===========================')
            except ClientError as e:
                if e.response['Error']['Code'] == '403':
                    print('=====================')
                    print('Failed to make connection to S3')
                    print('=====================')
                    return True
                elif e.response['Error']['Code'] == '404':
                    print('======================')
                    print('Bucket Not Found On S3. Exiting program')
                    print('======================')
                    return True
            try:
                client.head_object(Bucket=self.bucketName, Key=self.fileName)
                print('======================')
                print('File Already Exists On S3 Bucket')
                print('======================')
                return True
            except ClientError as e:
                print("Received error:", e)
                print(e.response['Error']['Code'])
                if e.response['Error']['Code'] == '403':
                    print('=====================')
                    print('Failed to make connection to S3')
                    print('=====================')
                    return True
                elif e.response['Error']['Code'] == '404':
                    print('======================')
                    print('File Not Found On S3 Bucket')
                    print('======================')
                    return False
        else:
            print('======================')
            print('Unable to make a connection to S3')
            print('======================')
            return True

class CreateBotoClient():
    def __init__(self,aws_access_key_id, aws_secret_access_key):
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_access_key_id = aws_access_key_id
    def createClient(self):

        try:
            client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                config=Config(signature_version='s3v4')
            )
            return client
        except ClientError as e:
            print('=====================')
            print('Failed to make connection to S3')
            print('=====================')
            return



# if __name__ == '__main__':
#     luigi.run(['SimpleTest','--workers','1','--local-scheduler'])
# python3 -m luigi --module Luigi_Test1 DownloadRejectLoanDataSetTask --awsaccesskeyid AKIAIXAAY2L4M64ZF5MQ --awssecretaccesskey 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 --local-scheduler --loanDataFile client.cfg --bucketName testbucketads