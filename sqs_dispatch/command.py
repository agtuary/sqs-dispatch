from __future__ import annotations
import os
import logging
from subprocess import Popen, PIPE, CalledProcessError
from typing import List, Tuple, Dict, Union

logger = logging.getLogger(__name__)


def execute(command: Union[str, List, Tuple], shell=True, env: Dict[str, str] = {}):
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
        print(stdout_line)
        output += stdout_line

    for stderr_line in iter(popen.stderr.readline, ""):
        print(stderr_line)
        error += stderr_line

    popen.stdout.close()
    popen.stderr.close()
    return_code = popen.wait()
    if return_code > 0:
        raise CalledProcessError(return_code, command, output=output, stderr=error)
    return output
