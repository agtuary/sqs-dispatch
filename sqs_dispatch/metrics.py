import logging
from contextlib import asynccontextmanager
from datadog import initialize, api
from time import time

initialize(api_host="https://app.datadoghq.eu")

logger = logging.getLogger("sqs_dispatch.metrics")


async def send_finished_metrics(event, duration, metric_tags=None):
    if metric_tags is None:
        metric_tags = {}
    try:
        api.Metric.send(
            metrics=[
                {
                    "metric": f"ag.sqs_dispatch.task.{event}",
                    "type": "count",
                    "points": [(int(time()), 1)],
                    "tags": metric_tags,
                },
                {
                    "metric": "ag.sqs_dispatch.task.duration",
                    "type": "count",
                    "points": [(int(time()), duration)],
                    "tags": metric_tags,
                },
            ]
        )
    except api.exceptions.ApiNotInitialized:
        logger.warning("Datadog API not initialized, not sending metrics")


@asynccontextmanager
async def capture_metrics(tags={}):
    start = time()
    try:
        yield
        duration = time() - start
        await send_finished_metrics("success", duration, tags)
    except Exception as e:
        duration = time() - start
        await send_finished_metrics(
            "failure",
            duration,
            {
                **tags,
                "error": e.__class__.__name__,
            },
        )
        raise
