#!/bin/bash
#                           PUBLIC DOMAIN NOTICE
#              National Center for Biotechnology Information
#
# This software is a "United States Government Work" under the
# terms of the United States Copyright Act.  It was written as part of
# the authors' official duties as United States Government employees and
# thus cannot be copyrighted.  This software is freely available
# to the public for use.  The National Library of Medicine and the U.S.
# Government have not placed any restriction on its use or reproduction.
#
# Although all reasonable efforts have been taken to ensure the accuracy
# and reliability of the software and data, the NLM and the U.S.
# Government do not and cannot warrant the performance or results that
# may be obtained by using this software or data.  The NLM and the U.S.
# Government disclaim all warranties, express or implied, including
# warranties of performance, merchantability or fitness for any particular
# purpose.
#
# Please cite NCBI in any work or product based on this material.
#
# cloud-job-submit.sh: Script to submit BLAST jobs on the cloud
#
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Tue 14 Sep 2021 09:32:15 PM EDT

set -uo pipefail
shopt -s nullglob

# Constants from ElasticBLAST source code
ELB_METADATA_DIR=metadata
ELB_NUM_JOBS_SUBMITTED=num_jobs_submitted.txt
K8S_JOB_GET_BLASTDB=get-blastdb
K8S_JOB_IMPORT_QUERY_BATCHES=import-query-batches
K8S_JOB_SUBMIT_JOBS=submit-jobs

AZCOPY_COPY='azcopy cp'
KUBECTL=kubectl

azcopy login --identity || { echo "ERROR: azcopy login failed"; exit 1; }

log() { ts=`date +'%F %T'`; printf '%s RUNTIME %s %f seconds\n' "$ts" "$1" "$2"; };
copy_job_logs_to_results_bucket() {
    ${KUBECTL} logs -l "app=$1" -c "$2" --timestamps --since=24h --tail=-1 >> logs
    ${AZCOPY_COPY} logs "${ELB_RESULTS}/logs/k8s-$1-$2.log"
    rm logs
}

TEST=${ELB_LOCAL_TEST:-}
if [ "x$TEST" == "x1" ]; then
    echo "Running in test mode"
    AZCOPY_COPY='cp'
    KUBECTL='echo kubectl'
    ELB_RESULTS=test
    ELB_CLUSTER_NAME=test-cluster
    ELB_USE_LOCAL_SSD=false
    mkdir -p test/metadata
    cp ../src/elastic_blast/templates/blast-batch-job.yaml.template test/metadata/job.yaml.template
    for ((i=0; i<1020; i++)) do printf 'batch_%03d.fa\n' "$i" >> test/metadata/batch_list.txt; done
    mkdir -p test/logs
    set -x
fi

# Wait for all previous setup jobs completion
while true; do
    s=`${KUBECTL} get jobs -l app=setup -o jsonpath='{.items[?(@.status.active)].metadata.name}'`
    [ $? -ne 0 ] || [ -n "$s" ] || break
    echo Waiting for jobs $s
    sleep 30
done

s=`${KUBECTL} get jobs -l app=setup -o jsonpath='{.items[?(@.status.conditions[?(@.type=="Failed")])].metadata.name}'`
if [[ -n "$s" ]]; then
    echo "Setup job(s) failed:" $s
    copy_job_logs_to_results_bucket setup "${K8S_JOB_GET_BLASTDB}"
    copy_job_logs_to_results_bucket setup "${K8S_JOB_IMPORT_QUERY_BATCHES}"
    exit 1
fi


# Get init-pv job logs
pods=`kubectl get pods -l job-name=init-pv -o jsonpath='{.items[*].metadata.name}'`
for pod in $pods; do
    for c in ${K8S_JOB_GET_BLASTDB} ${K8S_JOB_IMPORT_QUERY_BATCHES}; do
        ${KUBECTL} logs $pod -c $c --timestamps --since=24h --tail=-1 >> logs-init-pv
        ${AZCOPY_COPY} logs-init-pv ${ELB_RESULTS}/logs/k8s-$pod-$c.log
        rm logs-init-pv
    done
done

if ! $ELB_USE_LOCAL_SSD ; then
    echo Create ReadWriteMany PVC
    envsubst '${ELB_PD_SIZE}' </templates/pvc-rwm-aks.yaml.template >pvc-rwm-aks.yaml
    ${KUBECTL} apply -f pvc-rwm-aks.yaml
fi

# Debug job fail - set env variable ELB_DEBUG_SUBMIT_JOB_FAIL to non empty value
[ -n "${ELB_DEBUG_SUBMIT_JOB_FAIL:-}" ] && echo Job submit job failed for debug && exit 1

# Get template, batch list, and submit BLAST jobs
if ${AZCOPY_COPY} ${ELB_RESULTS}/${ELB_METADATA_DIR}/job.yaml.template . &&
${AZCOPY_COPY} ${ELB_RESULTS}/${ELB_METADATA_DIR}/batch_list.txt . ; then
    echo Submitting jobs
    i=0; j=0; job_dir_num=0; job_dir="jobs/$job_dir_num"
    start=`date +%s`
    mkdir -p $job_dir
    for batch in `cat batch_list.txt`; do
        if [[ $batch =~ batch_([0-9]{3,})\.fa ]]; then
            export JOB_NUM=${BASH_REMATCH[1]}
            export BLAST_ELB_BATCH_NUM=$i
            envsubst '${JOB_NUM} ${BLAST_ELB_BATCH_NUM}' <job.yaml.template >$job_dir/job_${JOB_NUM}.yaml
            
            if [ -n "${ELB_DEBUG:-}" ]; then
                echo "DEBUG: Job $i: $job_dir/job_${JOB_NUM}.yaml"
                echo "----------------------------------------------"
                cat $job_dir/job_${JOB_NUM}.yaml
            fi
        fi
        i=$[i + 1]
        j=$[j + 1]
        if [ $j -gt 99 ]; then
            j=0
            job_dir_num=$[job_dir_num + 1]
            job_dir="jobs/$job_dir_num"
            mkdir -p $job_dir
        fi
    done
    num_jobs=$i
    if [ $j -eq 0 ]; then
        job_dir_num=$[job_dir_num - 1]
    fi
    for ((i=0;i<=job_dir_num;i++)); do
        echo "jobs/$i/"
        ls "jobs/$i/"
        attempts=6
        while ! jobs=`${KUBECTL} apply -f "jobs/$i/"`; do
            attempts=$[attempts - 1]
            if [ $attempts -le 0 ]; then break; fi
            sleep 5
        done
    done
    end=`date +%s`
    log "submit-jobs" $(($end-$start))
    if [ $(( ($end-$start) )) -ne 0 ] ; then
        printf "SPEED to submit-jobs %f jobs/second\n" $(( $num_jobs/($end-$start) ))
    fi
    echo Submitted $num_jobs jobs
    echo $num_jobs > num_jobs && ${AZCOPY_COPY} num_jobs ${ELB_RESULTS}/${ELB_METADATA_DIR}/${ELB_NUM_JOBS_SUBMITTED}
    copy_job_logs_to_results_bucket submit "${K8S_JOB_SUBMIT_JOBS}"
    echo Done
else
    echo "Job file or batch list not found in Azure Blob Storage"
    exit 1
fi

exit 0
