from timeit import default_timer

from dash import Dash
from flask import Response, request
from prometheus_client import REGISTRY, Histogram
from prometheus_client.utils import INF
from prometheus_flask_exporter import PrometheusMetrics

from amora.version import VERSION


def register_metrics(dash: Dash) -> None:
    flask_app = dash.server
    metrics = PrometheusMetrics(app=flask_app, registry=REGISTRY)

    metrics.info("amora_version", "Amora version", version=VERSION)

    pathname_request_duration_metric = Histogram(
        "amora_dash_pages_location_pathname_change",
        "HTTP request duration, in seconds, related to an UI URL pathname change.",
        ("method", "pathname", "status"),
        registry=metrics.registry,
    )

    component_update_request_duration_metric = Histogram(
        "amora_dash_component_update",
        "HTTP request duration, in seconds, related to an UI component update.",
        ("method", "status", "inputs", "output"),
        unit="seconds",
        registry=metrics.registry,
    )

    component_update_response_size_metric = Histogram(
        "amora_dash_component_update_response_size",
        "HTTP response size, in bytes, related to an UI component update.",
        ("method", "status", "inputs", "output"),
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

    def after_request(response: Response) -> Response:
        if not hasattr(request, "prom_start_time"):
            raise ValueError
        total_time = max(default_timer() - request.prom_start_time, 0)

        if request.path == "/_dash-update-component":
            payload = request.get_json()
            if "_pages_location.pathname" in payload["changedPropIds"]:
                for i in payload["inputs"]:
                    if i["id"] == "_pages_location" and i["property"] == "pathname":
                        pathname_request_duration_metric.labels(
                            method=request.method,
                            pathname=i["value"],
                            status=response.status_code,
                        ).observe(total_time)
                        return response

            elif "url.pathname" in payload["changedPropIds"]:
                for i in payload["inputs"]:
                    if i["id"] == "url" and i["property"] == "pathname":
                        pathname_request_duration_metric.labels(
                            method=request.method,
                            pathname=i["value"],
                            status=response.status_code,
                        ).observe(total_time)
                        return response

            else:
                inputs = ":".join(i["id"] for i in payload["inputs"])
                metric_labels = dict(
                    method=request.method,
                    status=response.status_code,
                    inputs=inputs,
                    output=payload["output"],
                )
                component_update_request_duration_metric.labels(
                    **metric_labels
                ).observe(total_time)

                component_update_response_size_metric.labels(**metric_labels).observe(
                    response.content_length
                )
                return response

        return response

    def teardown_request(exception=None):
        pass

    flask_app.after_request(after_request)
    flask_app.teardown_request(teardown_request)
