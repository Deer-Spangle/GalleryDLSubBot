import datetime
import json
import logging
import pathlib
import uuid
from typing import Optional

import aiofiles
import aiorwlock
import deepmerge

from gallery_dl_sub_bot.run_cmd import run_cmd, Command

logger = logging.getLogger(__name__)


class GalleryDLManager:
    GALLERY_DL_PKG = "gallery-dl"
    GALLERY_DL_GITHUB = "https://github.com/mikf/gallery-dl"

    def __init__(self, config_path: Optional[str] = None):
        self.config_path: Optional[str] = config_path
        self.last_update: Optional[datetime.datetime] = None  # TODO: metrics, but would need to actually store it
        self.install_type: Optional[str] = None  # TODO: metric?
        self.rwlock = aiorwlock.RWLock()

    async def get_tool_version(self) -> str:
        logger.info("Checking gallery-dl version")
        async with self.rwlock.reader_lock:
            # TODO: would be cool to have a prometheus metric for this
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

    async def create_merged_config_file(self, new_config: dict) -> str:
        with open(self.config_path, "r") as f:
            base_config = json.load(f)
        merger = deepmerge.Merger(
            [
                (list, "override"),
                (dict, "merge"),
                (set, "override")
            ],
            ["override"],
            ["override"],
        )
        merged = merger.merge(base_config, new_config)
        config_dir = "store/configs"
        await aiofiles.os.makedirs(config_dir, exist_ok=True)
        config_filename = f"{config_dir}/{uuid.uuid4()}.json"
        async with aiofiles.open(config_filename, "w") as f:
            await f.write(json.dumps(merged, indent=2))
        return config_filename

    async def make_cmd(self, args: list[str]) -> Command:
        await self.check_install()
        return Command([self.GALLERY_DL_PKG, *args], lock=self.rwlock.reader_lock)

    def dl_args(self, link: str | list[str], dl_path: str) -> list[str]:
        archive_path = pathlib.Path(dl_path) / "archive.sqlite"
        args = []
        if self.config_path:
            if isinstance(link, str) or "-c" not in link:
                args += ["-c", self.config_path]
        link_args = link
        if isinstance(link, str):
            link_args = [link]
        args += [
            "--write-metadata",
            "--write-info-json",
            "-o", "output.skip=false",
            "-d", dl_path,
            "--download-archive", str(archive_path),
            *link_args,
        ]
        return args

    async def download_cmd(self, link: str | list[str], dl_path: str) -> Command:
        return await self.make_cmd(self.dl_args(link, dl_path))
