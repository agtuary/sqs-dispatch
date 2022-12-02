from __future__ import annotations
import os
import logging
from subprocess import Popen, PIPE, CalledProcessError
from typing import List, Tuple, Dict, Union, Callable, Optional
import asyncio

logger = logging.getLogger(__name__)


async def execute(
    command: Union[str, List[str]],
    env: Dict[str, str] = {},
    callback: Optional[Callable] = None,
):

    if isinstance(command, str):
        command = [command]

    # Create the subprocess; redirect the standard output
    # into a pipe.
    print(f"[cmd] executing {command}", flush=True)

    proc = await asyncio.create_subprocess_shell(
        " ".join(command + ["2>&1"]),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, **env},
    )

    async def monitor_pipe(name: str, pipe: asyncio.StreamReader):
        while True:
            # print("[cmd] polling for data", flush=True)

            if pipe.at_eof():
                print(f"[{name}] PIPE CLOSED", flush=True)
                return

            line = (await pipe.readline()).decode("utf-8", errors="ignore").rstrip()
            if line:
                print(f"[{name}] {line}", flush=True)
                if callback:
                    callback(name, line)

    await asyncio.gather(
        asyncio.create_task(monitor_pipe("out", proc.stdout)),
        asyncio.create_task(monitor_pipe("err", proc.stderr)),
    )

    await proc.wait()

    # Wait for the subprocess exit.
    if proc.returncode != 0:
        raise CalledProcessError(proc.returncode, command)

    return proc.returncode


def execute_sync(
    command: Union[str, List, Tuple],
    shell=True,
    env: Dict[str, str] = {},
    callback: Optional[Callable] = None,
):
    """
    Run a command in a subprocess.
    Args:
        command: Command to run.
        shell: If true, the command will be executed through the shell. Defaults to True.
        env: key=value pairs of environment variables to pass through
    Returns:
        The output of the command.
    """
    if isinstance(command, (list, tuple)):
        command = " ".join(command)
    logger.info("Executing command %s", command)
    output, error = "", ""
    popen = Popen(
        command,
        stdout=PIPE,
        stderr=PIPE,
        shell=shell,
        universal_newlines=True,
        env={**os.environ, **env},
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        stdout_line = stdout_line.strip()
        print("out", stdout_line)
        if callback:
            callback("out", stdout_line)
        output += stdout_line

    for stderr_line in iter(popen.stderr.readline, ""):
        stderr_line = stderr_line.strip()
        print("err", stderr_line)
        if callback:
            callback("err", stderr_line)
        error += stderr_line

    popen.stdout.close()
    popen.stderr.close()
    return_code = popen.wait()
    if return_code > 0:
        raise CalledProcessError(return_code, command, output=output, stderr=error)
    return output, error
