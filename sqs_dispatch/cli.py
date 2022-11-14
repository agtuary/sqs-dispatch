import asyncio
import click
import logging
from .queue import enqueue_message
from .worker import run_worker

logging.basicConfig(level=logging.INFO)
logging.getLogger("boto").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--queue", help="SQS queue to listen to", required=True, envvar="QUEUE_NAME"
)
@click.option("--debug/--no-debug", default=False, envvar="DEBUG")
@click.pass_context
def cli(ctx, debug, queue):
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug
    ctx.obj["QUEUE"] = queue

    print("Got", queue)

    if debug:
        logger.setLevel(logging.DEBUG)


@cli.command()
@click.pass_context
def process(ctx):
    queue = ctx.obj["QUEUE"]
    logger.info("Starting worker for queue %s", queue)
    asyncio.run(run_worker(queue))


@cli.command()
@click.argument("cmd", nargs=-1)
@click.pass_obj
def enqueue(obj, cmd):
    queue = obj["QUEUE"]
    message = {"command": list(cmd)}
    asyncio.run(enqueue_message(queue, message))
    logger.info("Enqueued %s", cmd)


def main():
    cli()
