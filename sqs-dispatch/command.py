import subprocess
import logging

logger = logging.getLogger(__name__)


def execute(cmd):
    logger.info("Executing command %s", cmd)
    popen = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        logger.info("[STDOUT] %s", stdout_line)

    for stderr_line in iter(popen.stderr.readline, ""):
        logger.error("[STDERR] %s", stderr_line)

    popen.stdout.close()
    popen.stderr.close()
    return_code = popen.wait()

    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)
