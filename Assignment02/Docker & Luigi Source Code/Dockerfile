FROM ubuntu:14.04

RUN apt-get update

RUN apt-get -y install python3  

RUN apt-get -y install python3-pip wget curl libssl-dev

RUN apt-get -y install software-properties-common 
RUN add-apt-repository ppa:git-core/ppa

ENV PYTHON_PIP_VERSION 9.0.1

RUN pip3 install --upgrade --force-reinstall "pip==$PYTHON_PIP_VERSION"

MAINTAINER Team6

RUN apt-get update

RUN mkdir /Docker_Luigi

RUN mkdir /Docker_Files

ADD requirements.txt /Docker_Luigi/requirements.txt

RUN pip3 install -r /Docker_Luigi/requirements.txt

#RUN pip install --no-cache-dir -r /Docker_Summary/requirements.txt

ADD ./Assignment2_Luigi_Pipeline.py /Docker_Luigi

ADD ./Loan_Download.py /Docker_Luigi

WORKDIR /Docker_Luigi

CMD python3 -m luigi --module Assignment2_Luigi_Pipeline UploadSummaryFilesToS3 --awsaccesskeyid $accesskey --awssecretaccesskey $secretkey --local-scheduler --loanDataFile $loanDataFile --rejectLoanDataFile $rejectLoanDataFile  --bucketName $bucket
