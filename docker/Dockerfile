FROM python:3.8.9-slim

RUN apt-get update && \
    apt-get -y install git findutils build-essential unzip wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ADD . .

RUN pip3 install keyrings.google-artifactregistry-auth --index-url=https://pypi.org/simple/
RUN pip3 install --upgrade pip && \
    pip3 install --require-hashes -r requirements.txt --no-deps --disable-pip-version-check && \
    pip3 cache purge
