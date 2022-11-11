from .queue import process_queue
from .command import execute
import click
import logging
import json


async def run_worker(queue: str):
    await process_queue(queue, lambda x: execute(x["command"]))
