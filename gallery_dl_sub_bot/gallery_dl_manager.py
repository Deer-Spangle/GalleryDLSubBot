import datetime
import logging
from typing import Optional

from gallery_dl_sub_bot.run_cmd import run_cmd

logger = logging.getLogger(__name__)


class GalleryDLManager:
    def __init__(self):
        self.last_update: Optional[datetime.datetime] = None

    async def update_tool(self) -> None:
        logger.info("Updating gallery-dl")
        await run_cmd(["pip", "install", "gallery-dl"])
        self.last_update = datetime.datetime.now(datetime.timezone.utc)

    def update_needed(self) -> bool:
        return self.last_update is None

    async def run(self, args: list[str]) -> str:
        if self.update_needed():
            await self.update_tool()
        resp = await run_cmd(["gallery-dl", *args])
        return resp
