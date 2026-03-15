#!/usr/bin/env python3
"""
benchmark/run_all.py — Automated ElasticBLAST Azure benchmark runner

Runs multiple test conditions, collects timing data, and saves results.
Usage: PYTHONPATH=src:$PYTHONPATH python benchmark/run_all.py
"""

import os, sys, json, time, subprocess, shlex, logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('benchmark')

# Configuration
RG = 'rg-elb-koc'
CLUSTER = 'elb-bench-a2'
STORAGE = 'stgelb'
ACR_RG = 'rg-elbacr'
ACR = 'elbacr'
REGION = 'koreacentral'
RESULTS_URL = f'https://{STORAGE}.blob.core.windows.net/results'
DATA_DIR = Path(__file__).parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

os.environ['AZCOPY_AUTO_LOGIN_TYPE'] = 'AZCLI'
os.environ['ELB_SKIP_DB_VERIFY'] = 'true'


def run(cmd, timeout=600):
    """Run a shell command and return (stdout, stderr, returncode, elapsed)."""
    t0 = time.time()
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    return r.stdout.strip(), r.stderr.strip(), r.returncode, time.time() - t0


def kubectl(cmd):
    return run(f'kubectl --context={CLUSTER} {cmd}')


def wait_cluster_ready():
    """Wait for AKS cluster to be in Succeeded state."""
    log.info('Waiting for cluster to be ready...')
    while True:
        out, _, _, _ = run(f'az aks show -g {RG} -n {CLUSTER} --query provisioningState -o tsv')
        if out == 'Succeeded':
            log.info('Cluster ready')
            return
        log.info(f'Cluster state: {out}')
        time.sleep(30)


def clean_results():
    run(f'azcopy rm "{RESULTS_URL}/" --recursive=true', timeout=60)


def clean_jobs():
    kubectl('delete jobs --all --ignore-not-found')
    kubectl('delete daemonset vmtouch-db-cache --ignore-not-found')


def get_job_timing(job_name):
    """Get job duration in seconds."""
    out, _, rc, _ = kubectl(f'get job {job_name} -o jsonpath="{{.status.completionTime}}"')
    if rc != 0:
        return None
    ct, _, _, _ = kubectl(f'get job {job_name} -o jsonpath="{{.status.startTime}}"')
    if not ct or not out:
        return None
    from datetime import datetime as dt
    try:
        start = dt.fromisoformat(ct.replace('Z', '+00:00').strip('"'))
        end = dt.fromisoformat(out.replace('Z', '+00:00').strip('"'))
        return (end - start).total_seconds()
    except Exception:
        return None


def run_benchmark(test_id, ini_path, description, warm=False):
    """Run a single benchmark and save results."""
    log.info(f'=== {test_id}: {description} ===')

    if not warm:
        clean_jobs()
    else:
        # Only clean blast + submit jobs, keep init-pv and PVC
        kubectl('delete jobs -l app=blast --ignore-not-found')
        kubectl('delete jobs -l app=submit --ignore-not-found')
        kubectl('delete job elb-finalizer --ignore-not-found')

    clean_results()
    time.sleep(5)

    # Run elastic-blast submit
    t0 = time.time()
    cmd = (f'cd {os.path.dirname(os.path.dirname(__file__))} && '
           f'PYTHONPATH=src:$PYTHONPATH ELB_SKIP_DB_VERIFY=true AZCOPY_AUTO_LOGIN_TYPE=AZCLI '
           f'python bin/elastic-blast submit --cfg {ini_path} --loglevel DEBUG')
    out, err, rc, submit_time = run(cmd, timeout=1800)
    log.info(f'Submit returned: rc={rc}, time={submit_time:.1f}s')

    if rc != 0:
        log.error(f'Submit failed: {err[-200:]}')
        result = {'test_id': test_id, 'success': False, 'error': err[-500:],
                  'submit_time_s': submit_time}
        _save(test_id, result)
        return result

    # Wait for all jobs to complete
    log.info('Waiting for BLAST jobs...')
    for _ in range(120):  # max 60 min
        out, _, _, _ = kubectl('get jobs -o jsonpath="{.items[*].status.conditions[0].type}"')
        jobs_out, _, _, _ = kubectl('get jobs --no-headers')
        total = len([l for l in jobs_out.split('\n') if l.strip()])
        completed = out.count('Complete')
        failed = out.count('Failed')
        if completed + failed >= total and total > 0:
            break
        time.sleep(30)

    total_time = time.time() - t0

    # Collect timings
    timings = {}
    for job in ['init-pv', 'submit-jobs', 'import-queries', 'elb-finalizer']:
        t = get_job_timing(job)
        if t is not None:
            timings[f'{job}_s'] = t

    # Get BLAST job timings
    blast_out, _, _, _ = kubectl('get jobs -l app=blast -o jsonpath="{.items[*].metadata.name}"')
    blast_jobs = [j.strip('"') for j in blast_out.split() if j.strip('"')]
    blast_times = []
    for bj in blast_jobs:
        t = get_job_timing(bj)
        if t is not None:
            blast_times.append(t)
    if blast_times:
        timings['blast_avg_s'] = sum(blast_times) / len(blast_times)
        timings['blast_max_s'] = max(blast_times)
        timings['blast_total_jobs'] = len(blast_times)

    timings['total_s'] = total_time
    timings['submit_cli_s'] = submit_time

    # Check results
    res_out, _, _, _ = run(f'azcopy list "{RESULTS_URL}/" 2>&1 | grep -c "out\\.gz"')
    result_count = int(res_out) if res_out.isdigit() else 0

    result = {
        'test_id': test_id,
        'description': description,
        'timestamp': datetime.now().isoformat(),
        'success': rc == 0 and result_count > 0,
        'warm_cluster': warm,
        'timings': timings,
        'result_files': result_count,
    }
    _save(test_id, result)
    log.info(f'Result: {json.dumps(timings, indent=2)}')
    return result


def _save(test_id, data):
    path = DATA_DIR / f'{test_id}.json'
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
    log.info(f'Saved: {path}')


def main():
    wait_cluster_ready()

    # Ensure credentials
    run(f'az aks get-credentials -g {RG} -n {CLUSTER} --overwrite-existing')

    ini = str(Path(__file__).parent / 'bench-a2.ini')

    # Test 1: Cold start (full init)
    log.info('='*60)
    r1 = run_benchmark('A2-cold-v2', ini, 'E32s_v3 x1, BlobNFS, cold start')

    # Test 2: Warm cluster (DB already loaded)
    log.info('='*60)
    r2 = run_benchmark('A2-warm-v2', ini, 'E32s_v3 x1, BlobNFS, warm cluster', warm=True)

    # Test 3: Warm cluster again (should be fastest)
    log.info('='*60)
    r3 = run_benchmark('A2-warm-v3', ini, 'E32s_v3 x1, BlobNFS, warm cluster (3rd run)', warm=True)

    # Summary
    log.info('='*60)
    log.info('BENCHMARK SUMMARY')
    for r in [r1, r2, r3]:
        if r and r.get('success'):
            t = r.get('timings', {})
            log.info(f"  {r['test_id']}: total={t.get('total_s',0):.0f}s, "
                     f"blast={t.get('blast_avg_s',0):.0f}s, "
                     f"init-pv={t.get('init-pv_s','skipped')}")
        elif r:
            log.info(f"  {r['test_id']}: FAILED")

    # Stop cluster + disable storage
    log.info('Stopping cluster and securing storage...')
    run(f'az aks stop -g {RG} -n {CLUSTER} --no-wait')
    run(f'az storage account update -n {STORAGE} --public-network-access Disabled -o none')
    log.info('Done. Cluster stopped, storage secured.')


if __name__ == '__main__':
    main()
