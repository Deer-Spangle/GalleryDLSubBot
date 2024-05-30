import datetime
import logging
import pathlib
from typing import Optional

from gallery_dl_sub_bot.run_cmd import run_cmd, Command

logger = logging.getLogger(__name__)


class GalleryDLManager:
    def __init__(self, config_path: Optional[str] = None):
        self.config_path: Optional[str] = config_path
        self.last_update: Optional[datetime.datetime] = None

    async def update_tool(self) -> None:
        logger.info("Updating gallery-dl")
        await run_cmd(["pip", "install", "gallery-dl"])
        self.last_update = datetime.datetime.now(datetime.timezone.utc)

    def update_needed(self) -> bool:
        return self.last_update is None

    async def check_update(self) -> None:
        if self.update_needed():
            await self.update_tool()

    async def make_cmd(self, args: list[str]) -> Command:
        await self.check_update()
        return Command(["gallery-dl", *args])

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
