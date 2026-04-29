# openapi

FROM python:3.11-slim

ARG version

LABEL Description="OpenAPI for NCBI ElasticBLAST Cloud Job Submission Module for Azure Kubernetes Service"
LABEL Version=${version}
LABEL Vendor="Microsoft"
LABEL Maintainer=moonchoi@microsoft.com

WORKDIR /app
COPY ./app /app

RUN apt-get -y -m update && \
    apt-get install -y apt-transport-https ca-certificates gnupg curl apt-utils

# Install kubectl (binary download — apt repo GPG v3 signatures rejected by Debian Trixie sqv)
RUN curl -LO "https://dl.k8s.io/release/v1.31.4/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin/kubectl

RUN apt-get -y -m update && \
    apt-get install -y zip unzip curl git wget jq && \
    pip3 install --no-cache-dir --upgrade pip setuptools && \
    pip3 install --no-cache-dir -r requirements.txt && \
    pip3 install --no-cache-dir --no-build-isolation git+https://github.com/dotnetpower/elastic-blast-azure.git@e3e9f51 && \
    rm -rf /var/lib/apt/lists/*

# Create and activate a virtual environment for Azure CLI
RUN python3 -m venv /opt/venv \
    && . /opt/venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir azure-cli --break-system-packages \
    && deactivate

ENV PATH="/opt/venv/bin:$PATH"

RUN wget -O azcopy_v10.tar.gz https://aka.ms/downloadazcopy-v10-linux && \
    tar -xf azcopy_v10.tar.gz --strip-components=1 && \
    cp ./azcopy /usr/bin && \
    rm -rf ./azcopy azcopy_v10.tar.gz

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
