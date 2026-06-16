# openapi

FROM python:3.11-slim

ARG version

LABEL Description="OpenAPI for NCBI ElasticBLAST Cloud Job Submission Module for Azure Kubernetes Service"
LABEL Version=${version}
LABEL Vendor="Microsoft"
LABEL Maintainer=moonchoi@microsoft.com

WORKDIR /app
COPY ./app /app
COPY patch_elastic_blast.py /tmp/patch_elastic_blast.py
COPY merge-sharded-results.sh /tmp/merge-sharded-results.sh

RUN apt-get -y -m update && \
    apt-get install -y apt-transport-https ca-certificates gnupg curl apt-utils

# Install kubectl (binary download — apt repo GPG v3 signatures rejected by Debian Trixie sqv)
RUN curl -LO "https://dl.k8s.io/release/v1.31.4/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin/kubectl

ARG ELB_REF=5b7ea2b

RUN apt-get -y -m update && \
    apt-get install -y zip unzip curl git wget jq && \
    pip3 install --no-cache-dir --upgrade pip setuptools && \
    pip3 install --no-cache-dir -r requirements.txt && \
    # Install elastic-blast + runtime deps in the SAME layer as app deps
    # so pip can resolve version conflicts in one pass.
    git clone --filter=blob:none --no-checkout https://github.com/dotnetpower/elastic-blast-azure.git /tmp/elb-src && \
    git -C /tmp/elb-src checkout ${ELB_REF} && \
    python3 /tmp/patch_elastic_blast.py /tmp/elb-src /tmp/merge-sharded-results.sh && \
    python3 -c "exec('from pathlib import Path\nblock = \"      tolerations:\\\\n      - key: workload\\\\n        operator: Equal\\\\n        value: blast\\\\n        effect: NoSchedule\\\\n\"\nd = Path(\"/tmp/elb-src/src/elastic_blast/templates\")\nfor p in d.glob(\"*-aks.yaml.template\"):\n    t = p.read_text()\n    if \"key: workload\" in t:\n        continue\n    t = t.replace(\"      restartPolicy:\", block + \"      restartPolicy:\")\n    t = t.replace(\"      nodeSelector:\\\\n        kubernetes.io/os: linux\", block + \"      nodeSelector:\\\\n        kubernetes.io/os: linux\")\n    p.write_text(t)')" && \
    pip3 install --no-cache-dir -r /tmp/elb-src/requirements/base.txt && \
        pip3 install --no-cache-dir \
            azure-mgmt-authorization==4.0.0 \
            azure-mgmt-compute==33.0.0 \
            azure-mgmt-containerservice==34.0.0 \
            azure-mgmt-storage==21.2.1 && \
    pip3 install --no-cache-dir --no-build-isolation /tmp/elb-src && \
    cp -a /tmp/elb-src/src/elastic_blast/templates/. /usr/local/lib/python3.11/site-packages/elastic_blast/templates/ && \
    install -m 0755 /tmp/elb-src/bin/elastic-blast /usr/local/bin/elastic-blast && \
    sed -i '1c#!/usr/local/bin/python3' /usr/local/bin/elastic-blast && \
    true && \
    rm -rf /var/lib/apt/lists/*

# Create and activate a virtual environment for Azure CLI (isolated from system)
RUN python3 -m venv /opt/venv \
    && . /opt/venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir azure-cli \
    && pip install --no-cache-dir --no-deps --no-build-isolation /tmp/elb-src \
    && cp -a /tmp/elb-src/src/elastic_blast/templates/. /opt/venv/lib/python3.11/site-packages/elastic_blast/templates/ \
    && rm -rf /tmp/elb-src \
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

# Drop privileges. uvicorn binds 8000 (>1024) so root is not required.
# /app holds runtime-writable artefacts (logs, tmp ConfigMap dumps), so
# transfer ownership to the unprivileged uid before switching user.
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --home-dir /app --no-create-home app \
    && chown -R app:app /app
USER app:app

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
