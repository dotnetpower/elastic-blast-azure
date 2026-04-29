"""
elastic_blast/azure_monitor.py — Application Insights integration for ElasticBLAST

Provides telemetry for BLAST search lifecycle events:
- Search submission (cluster size, DB name, batch count)
- Status transitions (submitting → running → success/failure)
- Cluster operations (create, delete, scale)
- Performance metrics (job throughput, runtime)

Activation: Set APPLICATIONINSIGHTS_CONNECTION_STRING environment variable.
When unset, all operations are no-ops (zero overhead).

Authors: Moon Hyuk Choi moonchoi@microsoft.com
"""

import os
import logging
import threading
import time
from typing import Dict, Optional

_CONN_STR = os.environ.get('APPLICATIONINSIGHTS_CONNECTION_STRING', '')
_initialized = False
_init_lock = threading.Lock()
_tracer = None
_meter = None

# Metric instruments (lazy-initialized)
_jobs_submitted = None
_jobs_completed = None
_jobs_failed = None
_cluster_create_duration = None
_blast_duration = None


def _ensure_initialized():
    """Lazy-initialize OpenTelemetry with Azure Monitor exporter."""
    global _initialized, _tracer, _meter
    global _jobs_submitted, _jobs_completed, _jobs_failed
    global _cluster_create_duration, _blast_duration

    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        _initialized = True

    if not _CONN_STR:
        logging.debug('Azure Monitor: disabled (no APPLICATIONINSIGHTS_CONNECTION_STRING)')
        return

    try:
        from opentelemetry import trace, metrics
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.metrics import MeterProvider
        from azure.monitor.opentelemetry.exporter import (
            AzureMonitorTraceExporter,
            AzureMonitorMetricExporter,
        )
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        # Traces
        trace_exporter = AzureMonitorTraceExporter(connection_string=_CONN_STR)
        trace_provider = TracerProvider()
        trace_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
        trace.set_tracer_provider(trace_provider)
        _tracer = trace.get_tracer('elastic_blast.azure')

        # Metrics
        metric_exporter = AzureMonitorMetricExporter(connection_string=_CONN_STR)
        metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=60000)
        meter_provider = MeterProvider(metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)
        _meter = metrics.get_meter('elastic_blast.azure')

        # Define instruments
        _jobs_submitted = _meter.create_counter('elb.jobs.submitted',
                                                 description='Total BLAST jobs submitted')
        _jobs_completed = _meter.create_counter('elb.jobs.completed',
                                                 description='BLAST jobs completed successfully')
        _jobs_failed = _meter.create_counter('elb.jobs.failed',
                                              description='BLAST jobs failed')
        _cluster_create_duration = _meter.create_histogram('elb.cluster.create_duration_s',
                                                            description='Cluster creation time in seconds')
        _blast_duration = _meter.create_histogram('elb.blast.duration_s',
                                                   description='BLAST job execution time in seconds')

        logging.info('Azure Monitor: initialized')
    except ImportError as e:
        logging.debug(f'Azure Monitor: SDK not available ({e})')
    except Exception as e:
        logging.warning(f'Azure Monitor: initialization failed ({e})')


# ---------------------------------------------------------------------------
# Public API — all no-ops when APPLICATIONINSIGHTS_CONNECTION_STRING is unset
# ---------------------------------------------------------------------------

def track_search_submitted(*, job_id: str, program: str, db: str,
                            num_jobs: int, num_nodes: int,
                            machine_type: str) -> None:
    """Record a search submission event."""
    _ensure_initialized()
    if _tracer:
        with _tracer.start_as_current_span('elb.search.submit') as span:
            span.set_attribute('elb.job_id', job_id)
            span.set_attribute('elb.program', program)
            span.set_attribute('elb.db', db)
            span.set_attribute('elb.num_jobs', num_jobs)
            span.set_attribute('elb.num_nodes', num_nodes)
            span.set_attribute('elb.machine_type', machine_type)
    if _jobs_submitted:
        _jobs_submitted.add(num_jobs, {'program': program, 'db': db})


def track_search_completed(*, job_id: str, succeeded: int, failed: int,
                            duration_s: float) -> None:
    """Record search completion metrics."""
    _ensure_initialized()
    if _tracer:
        with _tracer.start_as_current_span('elb.search.complete') as span:
            span.set_attribute('elb.job_id', job_id)
            span.set_attribute('elb.succeeded', succeeded)
            span.set_attribute('elb.failed', failed)
            span.set_attribute('elb.duration_s', duration_s)
    if _jobs_completed:
        _jobs_completed.add(succeeded)
    if _jobs_failed and failed > 0:
        _jobs_failed.add(failed)
    if _blast_duration:
        _blast_duration.record(duration_s)


def track_cluster_created(*, cluster_name: str, duration_s: float,
                           num_nodes: int, machine_type: str) -> None:
    """Record cluster creation event."""
    _ensure_initialized()
    if _tracer:
        with _tracer.start_as_current_span('elb.cluster.create') as span:
            span.set_attribute('elb.cluster_name', cluster_name)
            span.set_attribute('elb.duration_s', duration_s)
            span.set_attribute('elb.num_nodes', num_nodes)
            span.set_attribute('elb.machine_type', machine_type)
    if _cluster_create_duration:
        _cluster_create_duration.record(duration_s)


def track_cluster_deleted(*, cluster_name: str, duration_s: float) -> None:
    """Record cluster deletion event."""
    _ensure_initialized()
    if _tracer:
        with _tracer.start_as_current_span('elb.cluster.delete') as span:
            span.set_attribute('elb.cluster_name', cluster_name)
            span.set_attribute('elb.duration_s', duration_s)


def track_event(name: str, properties: Optional[Dict[str, str]] = None) -> None:
    """Record a generic custom event."""
    _ensure_initialized()
    if _tracer:
        with _tracer.start_as_current_span(name) as span:
            for k, v in (properties or {}).items():
                span.set_attribute(k, v)
