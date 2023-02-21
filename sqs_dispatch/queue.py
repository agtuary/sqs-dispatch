import json
import os
import logging
from aiobotocore.session import get_session
from typing import Coroutine
from pprint import pprint
from time import time
from traceback import print_exc
from .metrics import capture_metrics
from datadog import initialize, api

logger = logging.getLogger(__name__)


async def enqueue_message(queue_url: str, message: dict):
    session = get_session()
    async with session.create_client("sqs") as client:
        await client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


async def process_loop(queue_url: str, client, handler: Coroutine):
    # This loop wont spin really fast as there is
    # essentially a sleep in the receive_message call
    response = await client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,
    )

    start = time()

    if "Messages" not in response:
        logger.debug("No messages in response, moving on")
        return

    msg = response["Messages"][0]
    print("Received message:")
    pprint(msg)

    try:
        parsed_msg = json.loads(msg["Body"])
    except json.decoder.JSONDecodeError:
        logger.error("[%s] Message body is not JSON", msg["MessageId"])
        return

    if "command" not in parsed_msg:
        logger.error("[%s] No command in message, moving on", msg["MessageId"])
        return

    metric_tags = parsed_msg["tags"] if "tags" in parsed_msg else {}
    metric_tags = {**metric_tags, "queue_name": queue_url}

    try:
        logger.info(
            "Processing message %s with body %s",
            msg["MessageId"],
            parsed_msg,
        )

        async with capture_metrics(metric_tags):
            await handler(msg["MessageId"], parsed_msg)

        logger.info(
            "[%s] Finished processing message (took %ds), deleting message from queue",
            msg["MessageId"],
            time() - start,
        )

        await client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=msg["ReceiptHandle"],
        )
    except Exception as e:
        print_exc()
        logger.warn(
            "Failed to process message %s because %s (took %ds), skipping",
            msg["MessageId"],
            e,
            time() - start,
        )


async def process_queue(queue_url: str, handler: Coroutine):
    session = get_session()
    async with session.create_client("sqs") as client:
        while True:
            try:
                await process_loop(queue_url, client, handler)
            except KeyboardInterrupt:
                break

        print("Finished")
