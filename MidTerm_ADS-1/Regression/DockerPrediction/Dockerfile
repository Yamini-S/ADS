FROM ubuntu:16.04

MAINTAINER Team6

RUN apt-get update

RUN apt-get -y install python3-pip wget curl libssl-dev python3-tk

#RUN wget https://www.python.org/ftp/python/3.5.2/Python-3.5.2.tgz tar xzf Python-3.5.2.tgz cd Python-3.5.2 ./configure  make altinstall

RUN apt-get -y install software-properties-common 

#RUN apt-get install --reinstall ca-certificates

RUN add-apt-repository ppa:git-core/ppa

ENV PYTHON_PIP_VERSION 9.0.1

RUN pip3 install --upgrade --force-reinstall "pip==$PYTHON_PIP_VERSION"

RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

RUN mkdir /Docker_Regression

ADD requirements.txt /Docker_Regression/requirements.txt

RUN pip3 install -r /Docker_Regression/requirements.txt

RUN pip3 install --upgrade requests
#RUN pip install --no-cache-dir -r /Docker_Case1Part1/requirements.txt

ADD ./MidTerm_Prediction.py /Docker_Regression

WORKDIR /Docker_Regression

CMD python3 -u MidTerm_Prediction.py 
