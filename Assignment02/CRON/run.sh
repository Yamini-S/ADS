#!/bin/bash

sudo docker run -e "accesskey=<AccessKey>" -e  "secretkey=<secretkey>" -e "bucket=<bucketName>" -e "loanDataFile=loanDataFileDocker2.csv" -e "rejectLoanDataFile=rejectLoanDataFileDocker2.csv" assignment2_1 > /home/ubuntu/out.log
