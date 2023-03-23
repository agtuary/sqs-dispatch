import json
import logging
import sys
from time import time
from traceback import print_exc
from typing import Awaitable, Callable, Any

import botocore.exceptions
from aiobotocore.session import get_session

from sqs_dispatch.metrics import capture_metrics

logger = logging.getLogger("sqs_dispatch.queue")
AsyncFunc = Callable[[Any, Any], Awaitable[Any]]


async def enqueue_message(queue_url: str, message: dict):
    session = get_session()
    async with session.create_client("sqs") as client:
        await client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


async def process_loop(queue_url: str, client, handler: AsyncFunc):
    # This loop doesn't spin really fast as there is
    # essentially a sleep in the receive_message call
    response = await client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,
    )

    start = time()

    if "Messages" not in response:
        logger.info("No messages in response, moving on")
        return

    msg = response["Messages"][0]
    logger.info(f"Received message: {msg}")

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
        logger.warning(
            "Failed to process message %s because %s (took %ds), skipping",
            msg["MessageId"],
            e,
            time() - start,
        )


async def process_queue(queue_name: str, handler: AsyncFunc):
    session = get_session()
    async with session.create_client("sqs") as client:
        try:
            response = await client.get_queue_url(QueueName=queue_name)
        except botocore.exceptions.ClientError as err:
            if (
                err.response["Error"]["Code"]
                == "AWS.SimpleQueueService.NonExistentQueue"
            ):
                logger.error(f"Queue {queue_name} does not exist")
                sys.exit(1)
            else:
                raise

        queue_url = response["QueueUrl"]
        logger.info(f"Processing queue {queue_url}")
        while True:
            try:
                await process_loop(queue_url, client, handler)
            except KeyboardInterrupt:
                break

        print("Finished")
