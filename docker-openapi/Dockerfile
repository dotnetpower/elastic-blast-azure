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

ARG ELB_REF=dad3943

RUN apt-get -y -m update && \
    apt-get install -y zip unzip curl git wget jq && \
    pip3 install --no-cache-dir --upgrade pip setuptools && \
    pip3 install --no-cache-dir -r requirements.txt && \
    # Clone + install in editable mode so bin/elastic-blast can be installed
    # explicitly. `pip install git+...` does not honor `setup.cfg` `scripts =
    # bin/*` because the repo has no MANIFEST.in, so the bin/ directory is
    # excluded from the sdist that pip builds. We work around this by cloning
    # locally and copying the script into PATH after install.
    git clone --filter=blob:none --no-checkout https://github.com/dotnetpower/elastic-blast-azure.git /tmp/elb-src && \
    git -C /tmp/elb-src checkout ${ELB_REF} && \
    # Install elastic-blast runtime dependencies first. setup.cfg has no
    # 'install_requires' (it lives in requirements/base.txt), so a plain
    # 'pip install .' would otherwise leave the package importable but
    # unable to start (e.g. tenacity / boto3 / azure-storage-blob missing).
    pip3 install --no-cache-dir -r /tmp/elb-src/requirements/base.txt && \
    pip3 install --no-cache-dir --no-build-isolation /tmp/elb-src && \
    install -m 0755 /tmp/elb-src/bin/elastic-blast /usr/local/bin/elastic-blast && \
    # Pin the script's shebang to the system interpreter where elastic_blast
    # is actually installed. The /opt/venv venv (created below for azure-cli)
    # gets prepended to PATH, so the default `#!/usr/bin/env python3` would
    # otherwise resolve to /opt/venv/bin/python3 — which has no elastic_blast.
    sed -i '1c#!/usr/local/bin/python3' /usr/local/bin/elastic-blast && \
    rm -rf /tmp/elb-src && \
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

# Default azcopy auto-login to WORKLOAD so the container picks up the
# AKS-injected federated token directly via AZURE_CLIENT_ID /
# AZURE_TENANT_ID / AZURE_FEDERATED_TOKEN_FILE. AZCLI mode requires
# an active `az login` session AND a subscription role on the MI,
# which is fragile. WORKLOAD mode needs only data-plane RBAC (e.g.
# Storage Blob Data Contributor) on the target resource. Override
# at the deployment env to switch back if needed.
ENV AZCOPY_AUTO_LOGIN_TYPE=WORKLOAD

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
