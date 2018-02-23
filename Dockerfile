# VERSION 1.8.1-1ncubating-custom
# AUTHOR: vijay
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t sstumgdocker/docker-airflow .
# SOURCE: https://github.com/umg/sstumg-incubator-airflow/tree/v1-8-stable-add-dataflow-status

FROM python:2.7-stretch
#FROM phusion/baseimage


MAINTAINER Vijay_

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Enable this when want to install airflow based on version for apache-airflow/airflow
#ARG AIRFLOW_VERSION=1.8.0
ARG AIRFLOW_HOME=/usr/local/airflow




# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL en_US.UTF-8



RUN set -ex \
    && buildDeps=' \
        python-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        python-pip \
        python-requests \
        apt-utils \
        curl \
        netcat \
        locales \

    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install Cython \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install "requests[security]" \
    && pip install apache-airflow \
    && pip install -e .[crypto,celery,postgres,mysql,jdbc,gcp_api] \
    && pip install bigquery-python \
    #this can install from apache-airflow or airflow based on version
    #&& pip install airflow[crypto,celery,postgres,jdbc,gcp_api]==$AIRFLOW_VERSION \
    && pip install celery[redis]==3.1.17 \

    && echo 'deb http://deb.debian.org/debian jessie-backports main' > /etc/apt/sources.list.d/jessie-backports.list \
    && apt-get update -yqq \
    && apt-get install -yqq --fix-missing --no-install-recommends --target-release jessie-backports \
                openjdk-8-jre-headless \
                 ca-certificates-java \
                --assume-yes \
    #mongo shell

    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10 \
        && echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.0.list  \
        && apt-get update \
        && apt-get install -y --force-yes mongodb-org-shell=3.0.7 mongodb-org-tools=3.0.7 \


    ##cloud sql proxy

    && wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy \
    && chmod +x cloud_sql_proxy \
    && mv cloud_sql_proxy /usr/local/ \

    && curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz \
    && mkdir -p /usr/local/gcloud \
    && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
    && /usr/local/gcloud/google-cloud-sdk/install.sh \
    && PATH="$PATH:/usr/local/gcloud/google-cloud-sdk/bin" \
    && gcloud components install beta \
    && mkdir -p /opt/app \
    && chown -R airflow: ${AIRFLOW_HOME} \
    && chown -R airflow: /opt/app \
    && apt-get remove --purge -yqq $buildDeps \

    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base


ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

COPY script/entrypoint.sh /entrypoint.sh

#copy airflow configuration
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

#copy custom airflow plugins to plugins dir in airflow home
COPY plugins/* ${AIRFLOW_HOME}/plugins/

#copy requirements file for custom packages installation
COPY requirements.txt /requirements.txt



EXPOSE 8080 5555 8793 3306

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]

