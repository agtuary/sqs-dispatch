import json
import logging
from aiobotocore.session import get_session
from typing import Callable
from pprint import pprint
from time import time

logger = logging.getLogger(__name__)


async def enqueue_message(queue_url: str, message: dict, region: str = "us-west-2"):
    session = get_session()
    async with session.create_client("sqs", region_name=region) as client:
        await client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


async def process_queue(queue_url: str, handler: Callable, region: str = "us-west-2"):
    session = get_session()
    async with session.create_client("sqs", region_name=region) as client:
        while True:
            try:
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
                    continue

                msg = response["Messages"][0]
                print("Received message:")
                pprint(msg)

                try:
                    parsed_msg = json.loads(msg["Body"])
                except json.decoder.JSONDecodeError:
                    logger.error("[%s] Message body is not JSON", msg["MessageId"])
                    continue

                if "command" not in parsed_msg:
                    logger.error(
                        "[%s] No command in message, moving on", msg["MessageId"]
                    )
                    continue

                try:
                    logger.info(
                        "Processing message %s with body %s",
                        msg["MessageId"],
                        parsed_msg,
                    )
                    handler(parsed_msg)

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
                    # print_exc()
                    logger.warn(
                        "Failed to process message %s because %s (took %ds), skipping",
                        msg["MessageId"],
                        e,
                        time() - start,
                    )

            except KeyboardInterrupt:
                break

        print("Finished")
