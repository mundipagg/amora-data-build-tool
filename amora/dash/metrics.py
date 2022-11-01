from timeit import default_timer
from typing import Optional
from uuid import uuid4

from dash import Dash
from flask import Response, request
from prometheus_client import REGISTRY, Histogram
from prometheus_client.utils import INF
from prometheus_flask_exporter import PrometheusMetrics

from amora.logger import logger
from amora.version import VERSION


def add_prometheus_metrics(dash: Dash) -> None:
    flask_app = dash.server
    metrics = PrometheusMetrics(app=flask_app, registry=REGISTRY, export_defaults=False)

    metrics.info("amora_version", "Amora version", version=VERSION)

    pathname_request_duration_metric = Histogram(
        "amora_dash_page_pathname_change_duration",
        "HTTP request duration, in seconds, related to an UI URL pathname change.",
        ("method", "status"),
        unit="seconds",
        registry=metrics.registry,
    )

    component_update_request_duration_metric = Histogram(
        "amora_dash_component_update_duration",
        "HTTP request duration, in seconds, related to an UI component update.",
        ("method", "status"),
        unit="seconds",
        registry=metrics.registry,
    )

    component_update_response_size_metric = Histogram(
        "amora_dash_component_update_response_size",
        "HTTP response size, in bytes, related to an UI component update.",
        ("method", "status"),
        buckets=[
            1000,
            5000,
            10_000,
            100_000,
            1_000_000,
            10_000_000,
            100_000_000,
            INF,
        ],
        unit="bytes",
        registry=metrics.registry,
    )

    def before_request():
        request.metrics_start_time = default_timer()
        request.uid = uuid4()

    def after_request(response: Response) -> Response:
        start_time: Optional[float] = getattr(request, "metrics_start_time")
        if not start_time:
            logger.info("Ignoring request without start timestamp")
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

    def teardown_request(exception=None):
        pass

    flask_app.before_request(before_request)
    flask_app.after_request(after_request)
    flask_app.teardown_request(teardown_request)
