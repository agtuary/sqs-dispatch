from aiobotocore.session import get_session
import logging
import json
from typing import Callable

logger = logging.getLogger(__name__)


async def enqueue_message(queue_url: str, message: dict, region: str = "us-west-2"):
    session = get_session()
    async with session.create_client("sqs", region_name=region) as client:
        await client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


async def process_queue(
    queue_url: str, handler: Callable, region: str = "us-west-2", timeout: int = 30
):
    session = get_session()
    async with session.create_client("sqs", region_name=region) as client:
        while True:
            try:
                # This loop wont spin really fast as there is
                # essentially a sleep in the receive_message call
                response = await client.receive_message(
                    QueueUrl=queue_url,
                    VisibilityTimeout=timeout,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20,
                )

                if "Messages" not in response:
                    logger.debug("No messages in response, moving on")
                    continue

                msg = response["Messages"][0]
                print(f'Got msg "{msg}"')

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
                        "[%s] Finished processing message, deleting message from queue",
                        msg["MessageId"],
                    )
                    await client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                except Exception as e:
                    # print_exc()
                    logger.warn(
                        "Failed to process message %s because %s, skipping",
                        msg["MessageId"],
                        e,
                    )

            except KeyboardInterrupt:
                break

        print("Finished")
