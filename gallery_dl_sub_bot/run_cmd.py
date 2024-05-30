import asyncio
import logging
from asyncio import StreamReader
from asyncio.subprocess import Process
from typing import TypeVar, Optional, Tuple, AsyncIterator

import aiostream.stream

T = TypeVar('T')
logger = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 60 * 60
DEFAULT_UPDATE_TIMEOUT = 30 * 60
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
            line = line_bytes.decode().rstrip("\n")
            logger.info(prefix + line)
            lines.append(line)
    except asyncio.TimeoutError:
        logger.error("STREAM TIMEOUT %s", prefix)
    return "\n".join(lines)


async def iter_stream(
        stream: StreamReader,
        timeout: Optional[int] = DEFAULT_UPDATE_TIMEOUT,
        prefix: Optional[str] = None,
) -> AsyncIterator[tuple[StreamReader, str]]:
    if prefix is None:
        prefix = ""
    else:
        prefix += ": "
    try:
        while True:
            line_bytes = await asyncio.wait_for(stream.readline(), timeout)
            if line_bytes == b"":
                # EOF reached
                logger.debug("Stream ended", )
                break
            line = line_bytes.decode().rstrip("\n")
            logger.info(prefix + line)
            yield stream, line
    except asyncio.TimeoutError as e:
        logger.error("STREAM TIMEOUT")
        raise e


def _printable_cmd(args: list[str]) -> str:
    safe_args = []
    for arg in args:
        arg = arg.replace('"', '\"')
        if " " in arg:
            arg = f"\"{arg}\""
        safe_args.append(arg)
    return " ".join(safe_args)


async def run_cmd(args: list[str], timeout: int = DEFAULT_TIMEOUT) -> str:
    proc = await asyncio.create_subprocess_exec(
        *args,
        limit=1024 * 1024 * 5,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    logger.info("Running subprocess: %s", _printable_cmd(args))
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


class Command:

    def __init__(
            self,
            args: list[str],
            stdout_timeout: Optional[int] = DEFAULT_UPDATE_TIMEOUT,
            stderr_timeout: Optional[int] = DEFAULT_TIMEOUT,
    ):
        self.args = args
        self.stdout_timeout = stdout_timeout
        self.stderr_timeout = stderr_timeout
        self.proc: Optional[Process] = None

    @property
    def printable_args(self) -> str:
        return _printable_cmd(self.args)

    async def run_iter(self) -> AsyncIterator[str]:
        self.proc = await asyncio.create_subprocess_exec(
            *self.args,
            limit=1024 * 1024 * 5,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout = self.proc.stdout
        stderr = self.proc.stderr
        stderr_lines = []
        logger.info("Running subprocess: %s", self.printable_args)
        try:
            combine = aiostream.stream.merge(
                iter_stream(stdout, self.stdout_timeout, "stdout"),
                iter_stream(stderr, self.stderr_timeout, "stderr"),
            )
            async with combine.stream() as streamer:
                async for (stream, line) in streamer:
                    if stream == stdout:
                        yield line
                    if stream == stderr:
                        stderr_lines.append(line)
            # A short timeout to close the subprocess, because the above should have ran it to completion.
            await asyncio.wait_for(self.proc.communicate(), timeout=CLOSE_TIMEOUT)
            return_code = self.proc.returncode
        except asyncio.TimeoutError:
            logger.error("Subprocess timed out, killing: %s", self.printable_args)
            try:
                self.proc.kill()
            except OSError:
                pass
            raise CmdException("Task timed out")
        if return_code != 0:
            logger.warning("Subprocess returned exit code %s: %s", return_code, self.printable_args)
            all_stderr = "\n".join(stderr_lines)
            raise CmdException(f"Task returned exit code {return_code}. stderr: {all_stderr}")

    def kill(self) -> None:
        if self.proc is not None:
            self.proc.kill()
