from timeit import default_timer
from typing import Optional

from dash import Dash
from flask import Response, g, request
from prometheus_client import CollectorRegistry, Histogram
from prometheus_client.utils import INF
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics

from amora.dash.config import settings
from amora.logger import logger
from amora.version import VERSION


def add_prometheus_metrics(dash: Dash) -> None:
    flask_app = dash.server
    registry = CollectorRegistry()
    if settings.DEBUG:
        metrics = PrometheusMetrics(
            app=flask_app, registry=registry, export_defaults=False
        )
    else:
        metrics = GunicornPrometheusMetrics(
            app=flask_app, registry=registry, export_defaults=False
        )

    metrics.info("amora_version", "Amora version", version=VERSION)

    pathname_request_duration_metric = Histogram(
        name="amora_dash_page_pathname_change_duration",
        documentation="HTTP request duration, in seconds, related to an UI URL pathname change.",
        labelnames=("method", "status"),
        unit="seconds",
        registry=metrics.registry,
    )

    component_update_request_duration_metric = Histogram(
        name="amora_dash_component_update_duration",
        documentation="HTTP request duration, in seconds, related to an UI component update.",
        labelnames=("method", "status"),
        unit="seconds",
        registry=metrics.registry,
    )

    component_update_response_size_metric = Histogram(
        name="amora_dash_component_update_response_size",
        documentation="HTTP response size, in bytes, related to an UI component update.",
        labelnames=("method", "status"),
        buckets=[*settings.METRICS_COMPONENT_UPDATE_RESPONSE_SIZE_BUCKETS, INF],
        unit="bytes",
        registry=metrics.registry,
    )

    def before_request():
        g.metrics_start_time = default_timer()

    def after_request(response: Response) -> Response:
        start_time: Optional[float] = g.pop("metrics_start_time", None)
        if not start_time:
            return response

        total_time = max(default_timer() - start_time, 0)

        if request.path != "/_dash-update-component":
            return response

        payload: dict = request.get_json()  # type: ignore
        if "_pages_location.pathname" in payload["changedPropIds"]:
            for i in payload["inputs"]:
                if i["id"] == "_pages_location" and i["property"] == "pathname":
                    pathname_request_duration_metric.labels(
                        method=request.method,
                        status=response.status_code,
                    ).observe(total_time)
                    logger.info(
                        "UI page location change", extra=dict(urlpath=i["value"])
                    )
                    return response

        elif "url.pathname" in payload["changedPropIds"]:
            return response

        else:
            inputs = ":".join(i["id"] for i in payload["inputs"])
            logger.info(
                "Component update request",
                extra=dict(
                    inputs=inputs,
                    output=payload["output"],
                    response_size=response.content_length,
                    duration=total_time,
                ),
            )
            component_update_request_duration_metric.labels(
                method=request.method,
                status=response.status_code,
            ).observe(total_time)
            component_update_response_size_metric.labels(
                method=request.method,
                status=response.status_code,
            ).observe(response.content_length)

            return response
        return response

    flask_app.before_request(before_request)
    flask_app.after_request(after_request)
