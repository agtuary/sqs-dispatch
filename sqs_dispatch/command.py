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
    proc = await asyncio.create_subprocess_shell(
        " ".join(command),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, **env},
    )

    while True:
        if proc.stdout.at_eof() and proc.stderr.at_eof():
            break

        stdout = (await proc.stdout.readline()).decode()
        if stdout:
            stdout = stdout.strip()
            print(f"[stdout] {stdout}", flush=True)
            if callback:
                callback("out", stdout)

        stderr = (await proc.stderr.readline()).decode()
        if stderr:
            stderr = stderr.strip()
            print(f"[sdterr] {stderr}", flush=True)
            if callback:
                callback("err", stderr)

        if not stdout and not stderr:
            # Avoid rapidly checking if no output is available.
            await asyncio.sleep(1)

    await proc.communicate()

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
