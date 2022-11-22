from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics

from amora.dash.config import settings


def when_ready(server):
    GunicornPrometheusMetrics.start_http_server_when_ready(settings.METRICS_PORT)


def child_exit(server, worker):
    GunicornPrometheusMetrics.mark_process_dead_on_child_exit(worker.pid)
