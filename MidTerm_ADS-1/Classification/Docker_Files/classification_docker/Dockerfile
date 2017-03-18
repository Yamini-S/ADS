FROM python:3.5

MAINTAINER Team6

RUN apt-get update

RUN mkdir /Docker_Classification

ADD requirements.txt /Docker_Classification/requirements.txt

RUN pip install -r /Docker_Classification/requirements.txt

#RUN pip install --no-cache-dir -r /Docker_Classification/requirements.txt

ADD ./Midterm-Classification.py /Docker_Classification

WORKDIR /Docker_Classification

CMD python Midterm-Classification.py 


