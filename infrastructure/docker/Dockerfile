FROM python:3.9-slim

RUN apt-get update && \
    apt-get -y install git findutils build-essential unzip wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ADD requirements.txt requirements.txt

RUN pip3 install --upgrade pip && \
    pip3 install --require-hashes -r requirements.txt --no-deps --disable-pip-version-check && \
    pip3 cache purge

ADD . .
