import asyncio
import logging
from asyncio import StreamReader
from asyncio.subprocess import Process
from typing import TypeVar, Optional, Tuple

T = TypeVar('T')
logger = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 5 * 60
CLOSE_TIMEOUT = 10


class CmdException(Exception):
    pass


async def log_output(proc: Process, timeout: int = DEFAULT_TIMEOUT) -> Tuple[str, str]:
    return await asyncio.gather(
        log_stream(proc.stdout, timeout, "stdout"),
        log_stream(proc.stderr, timeout, "stderr")
    )


async def log_stream(stream: StreamReader, timeout: int = DEFAULT_TIMEOUT, prefix: Optional[str] = None) -> str:
    lines = []
    if prefix is None:
        prefix = ""
    else:
        prefix += ": "
    try:
        while True:
            line_bytes = await asyncio.wait_for(stream.readline(), timeout)
            if line_bytes == b"":
                # EOF reached
                logger.debug("Stream ended %s", prefix)
                break
            line = line_bytes.decode()
            logger.info(prefix + line)
            lines.append(line)
    except asyncio.TimeoutError:
        logger.error("STREAM TIMEOUT %s", prefix)
    return "\n".join(lines)


async def run_cmd(args, timeout: int = DEFAULT_TIMEOUT) -> str:
    proc = await asyncio.create_subprocess_exec(
        *args,
        limit=1024 * 1024 * 5,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    logger.debug("Running subprocess: %s", args)
    try:
        stdout, stderr = await asyncio.wait_for(log_output(proc), timeout=timeout)
        # A short timeout to close the subprocess, because the above should have ran it to completion.
        await asyncio.wait_for(proc.communicate(), timeout=CLOSE_TIMEOUT)
        return_code = proc.returncode
    except asyncio.TimeoutError:
        logger.error("Subprocess timed out, killing: %s", args)
        try:
            proc.kill()
        except OSError:
            pass
        raise CmdException("Task timed out")
    if return_code != 0:
        logger.warning("Subprocess returned exit code %s: %s", return_code, args)
        raise CmdException(f"Task returned exit code {return_code}. stderr: {stderr}")
    return stdout
