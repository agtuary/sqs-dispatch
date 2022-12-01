import click
import json
import logging
import os

from .command import execute
from .queue import process_queue

logger = logging.getLogger(__name__)


async def run_worker(queue: str):
    async def process_message(message_id: str, message: dict):
        logger.info("Processing message %s with body %s", message_id, message)
        await execute(message["command"], env={"SQS_MESSAGE_ID": message_id})

    await process_queue(queue, process_message)
