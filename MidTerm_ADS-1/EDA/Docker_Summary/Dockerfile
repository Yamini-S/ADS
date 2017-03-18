FROM python:3.5

MAINTAINER Team6

RUN apt-get update

RUN mkdir /Docker_Summary

ADD requirements.txt /Docker_Summary/requirements.txt

RUN pip install -r /Docker_Summary/requirements.txt

#RUN pip install --no-cache-dir -r /Docker_Summary/requirements.txt

ADD ./MidTerm_Summary.py /Docker_Summary

WORKDIR /Docker_Summary

CMD python -u MidTerm_Summary.py 


