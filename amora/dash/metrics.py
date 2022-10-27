from dash import Dash
from prometheus_flask_exporter import PrometheusMetrics

from amora.version import VERSION


def register_metrics(dash: Dash) -> None:
    metrics = PrometheusMetrics(app=dash.server)

    metrics.info("amora_version", "Amora version", version=VERSION)
