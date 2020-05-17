# VERSION 0.1.0
# AUTHOR: Manu NALEPA
# DESCRIPTION: Crypto Data container
# BUILD: docker build --rm nalepae/crypto-data .
# SOURCE: https://github.com/nalepae/crypto-data

FROM python:3.7-slim-buster

COPY crypto_data /tmp/crypto_data/crypto_data
COPY setup.py /tmp/crypto_data

RUN apt-get update \
    && apt-get install -y build-essential libffi-dev libssl-dev --no-install-recommends \
    && pip install /tmp/crypto_data \
    && apt-get purge -y --auto-remove build-essential libffi-dev libssl-dev