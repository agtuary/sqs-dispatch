import asyncio
from aiobotocore.session import get_session
import sys
import botocore.exceptions
from .worker import run_worker
from .queue import enqueue
import click
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("boto").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--queue", help="SQS queue to listen to", required=True)
@click.option("--debug/--no-debug", default=False)
@click.pass_context
def cli(ctx, debug, queue):
    ctx.ensure_object(dict)
    if debug:
        logger.setLevel(logging.DEBUG)
    ctx.obj["DEBUG"] = debug
    ctx.obj["QUEUE"] = queue


@cli.command()
@click.pass_context
def process(ctx):
    queue = ctx.obj["QUEUE"]
    logger.info("Starting worker for queue %s", queue)
    asyncio.run(run_worker(queue))


@cli.command()
@click.argument("COMMAND", nargs=-1)
@click.pass_context
def enqueue(ctx, command):
    queue = ctx.obj["QUEUE"]

    async def run_enqueue():
        await enqueue(queue, {"command": list(command)})

    asyncio.run(run_enqueue())
    logger.info("Enqueued %s", command)


def main():
    cli(auto_envvar_prefix="SQS_DISPATCH")
