import click
import json
import logging

from .command import execute
from .queue import process_queue


async def run_worker(queue: str):
    await process_queue(
        queue, lambda id, x: execute(x["command"], env={"SQS_MESSAGE_ID": id})
    )
