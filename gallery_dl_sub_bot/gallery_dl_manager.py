import datetime
import logging
import pathlib
from typing import Optional

import aiorwlock

from gallery_dl_sub_bot.run_cmd import run_cmd, Command

logger = logging.getLogger(__name__)


class GalleryDLManager:
    GALLERY_DL_PKG = "gallery-dl"
    GALLERY_DL_GITHUB = "https://github.com/mikf/gallery-dl"

    def __init__(self, config_path: Optional[str] = None):
        self.config_path: Optional[str] = config_path
        self.last_update: Optional[datetime.datetime] = None
        self.install_type: Optional[str] = None
        self.rwlock = aiorwlock.RWLock()

    async def get_tool_version(self) -> str:
        logger.info("Checking gallery-dl version")
        async with self.rwlock.reader_lock:
            pkg_info = await run_cmd(["pip", "show", self.GALLERY_DL_PKG])
            version_line = [line for line in pkg_info.split("\n") if line.startswith("Version: ")]
            if not version_line:
                return "Unknown"
            version = version_line[0].removeprefix("Version: ")
            return version

    async def install_tool(self) -> None:
        logger.info("Installing gallery-dl")
        async with self.rwlock.writer_lock:
            await run_cmd(["pip", "install", self.GALLERY_DL_PKG])
            self.last_update = datetime.datetime.now(datetime.timezone.utc)
            self.install_type = "stable"

    async def update_tool(self) -> None:
        logger.info("Updating gallery-dl")
        async with self.rwlock.writer_lock:
            await run_cmd(["pip", "install", "-U", self.GALLERY_DL_PKG, "--force-reinstall"])
            self.last_update = datetime.datetime.now(datetime.timezone.utc)
            self.install_type = "stable"

    async def update_tool_prerelease(self) -> None:
        logger.info("Updating gallery-dl to dev version")
        async with self.rwlock.writer_lock:
            await run_cmd(["pip", "install", "-U", "--force-reinstall", f"git+{self.GALLERY_DL_GITHUB}"])
            self.last_update = datetime.datetime.now(datetime.timezone.utc)
            self.install_type = "dev"

    def update_needed(self) -> bool:
        return self.last_update is None

    async def check_install(self) -> None:
        if self.last_update is None:
            await self.install_tool()

    async def make_cmd(self, args: list[str]) -> Command:
        await self.check_install()
        return Command([self.GALLERY_DL_PKG, *args], lock=self.rwlock.reader_lock)

    def dl_args(self, link: str, dl_path: str) -> list[str]:
        archive_path = pathlib.Path(dl_path) / "archive.sqlite"
        args = []
        if self.config_path:
            args += ["-c", self.config_path]
        args += [
            "--write-metadata",
            "--write-info-json",
            "-o", "output.skip=false",
            "-d", dl_path,
            "--download-archive", str(archive_path),
            link,
        ]
        return args

    async def download_cmd(self, link: str, dl_path: str) -> Command:
        return await self.make_cmd(self.dl_args(link, dl_path))
