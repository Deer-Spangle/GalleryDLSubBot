import asyncio
import dataclasses
import datetime
import glob
import html
import logging
import os
import typing
import uuid
from contextlib import asynccontextmanager
from typing import Optional, AsyncIterator

import aiofiles.os
import aioshutil
from prometheus_client import Counter

from gallery_dl_sub_bot.image_conversion import convert_if_webp
from gallery_dl_sub_bot.link_fixer import link_to_str
from gallery_dl_sub_bot.run_cmd import Command, run_cmd

if typing.TYPE_CHECKING:
    from gallery_dl_sub_bot.subscription_manager import SubscriptionManager


logger = logging.getLogger(__name__)

ZIP_SIZE_LIMIT = "1500m"
subscription_posts_made = Counter(
    "gallerydlsubbot_subscription_posts_made_total",
    "Total number of posts made by subscription updates",
)


class ActiveDownload:
    def __init__(self, dl: "Download", lines_at_start: list[str]) -> None:
        self.dl = dl
        self.lines_at_start = lines_at_start
        self.lines_so_far = []
        self.task = None
        self.complete = False
        self.command: Optional[Command] = None

    def kill(self) -> None:
        if self.command is not None:
            self.command.kill()

    async def run(self) -> AsyncIterator[list[str]]:
        yield self.lines_at_start
        # Create the download
        self.command = await self.dl.dl_manager.download_cmd(self.dl.link, self.dl.path)
        # Watch the download progress
        try:
            async for line in self.command.run_iter():
                self.lines_so_far.append(line)
                yield [line]
        except Exception as e:
            logger.warning("Failed to run active download: ", exc_info=e)
        # Send the updates out to any subscribers
        try:
            await self.dl.send_new_items(self.lines_so_far)
        except Exception as e:
            logger.warning("Failed to send %s new items for link: %s", len(self.lines_so_far), self.dl.link, exc_info=e)
        finally:
            self.complete = True
            self.dl.update_num_files()
            self.dl.sub_manager.save()

    async def track(self) -> AsyncIterator[list[str]]:
        yield self.lines_at_start
        high_water_mark = 0
        while not self.complete:
            new_lines = self.lines_so_far[high_water_mark:]
            high_water_mark = high_water_mark + len(new_lines)
            yield new_lines
            await asyncio.sleep(1)
        last_lines = self.lines_so_far[high_water_mark:]
        yield last_lines


class Download:

    def __init__(
            self,
            link: str | list[str],
            path: str,
            last_check_date: datetime.datetime,
            sub_manager: "SubscriptionManager",
    ) -> None:
        """
        Creates a Download object, which describes a link to be downloaded
        :param link: A link to a feed as a string, or a list containing a link and additional arguments to gallery-dl
        :param path: The path which the feed was downloaded into
        :param last_check_date: The last date that the download was checked for completeness
        :param sub_manager: Reference to the subscription manager
        """
        self.link = link
        self.path = path
        self.last_check_date = last_check_date
        self.sub_manager = sub_manager
        self.dl_manager = sub_manager.dl_manager
        self.zip_lock = asyncio.Lock()
        self.active_download: Optional[ActiveDownload] = None
        self.num_files = len(self.list_files())

    @property
    def link_str(self) -> str:
        return link_to_str(self.link)

    def list_files(self) -> list[str]:
        all_files = glob.glob(self.path + '/**/*.*', recursive=True)
        img_files = [f for f in all_files if os.path.isfile(f) and not (f.endswith(".json") or f.endswith(".sqlite"))]
        return sorted(img_files)

    def update_num_files(self) -> int:
        self.num_files = len(self.list_files())
        return self.num_files

    async def send_new_items(self, new_items: list[str]) -> None:
        pass

    def download(self) -> AsyncIterator[list[str]]:
        active_download = self.active_download
        if active_download is None or active_download.complete:
            new_download = ActiveDownload(self, self.list_files())
            self.active_download = new_download
            self.last_check_date = datetime.datetime.now(datetime.timezone.utc)
            return new_download.run()
        return active_download.track()

    @asynccontextmanager
    async def zip(self, filename: str) -> AsyncIterator[list[str]]:
        zip_dir = f"store/zips/{uuid.uuid4()}"
        await aiofiles.os.makedirs(zip_dir, exist_ok=True)
        zip_path = os.path.abspath(f"{zip_dir}/{filename}.zip")
        async with self.zip_lock:
            try:
                await run_cmd(["zip", "-r", "-s", ZIP_SIZE_LIMIT, zip_path, "./"], cwd=self.path)
                zip_files = await aiofiles.os.listdir(zip_dir)
                zip_paths = [f"{zip_dir}/{filename}" for filename in zip_files]
                yield sorted(zip_paths)
            finally:
                await aioshutil.rmtree(zip_dir)


class CompleteDownload(Download):

    def to_json(self) -> dict:
        return {
            "link": self.link,
            "path": self.path,
            "last_check_date": self.last_check_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict, sub_manager: "SubscriptionManager") -> "CompleteDownload":
        return cls(
            data["link"],
            data["path"],
            datetime.datetime.fromisoformat(data["last_check_date"]),
            sub_manager,
        )


@dataclasses.dataclass
class SubscriptionDestination:
    chat_id: int
    creator_id: int
    created_date: datetime.datetime
    paused: bool
    subscription: Optional["Subscription"] = None

    def to_json(self) -> dict:
        return {
            "chat_id": self.chat_id,
            "creator_id": self.creator_id,
            "created_date": self.created_date.isoformat(),
            "paused": self.paused,
        }

    @classmethod
    def from_json(cls, data: dict) -> "SubscriptionDestination":
        return cls(
            data["chat_id"],
            data["creator_id"],
            datetime.datetime.fromisoformat(data["created_date"]),
            data.get("paused", False),
        )


class Subscription(Download):

    def __init__(
            self,
            link: str | list[str],
            path: str,
            last_check_date: datetime.datetime,
            sub_manager: "SubscriptionManager",
            destinations: list[SubscriptionDestination],
            failed_checks: int,
            last_successful_check_date: datetime.datetime,
    ) -> None:
        super().__init__(link, path, last_check_date, sub_manager)
        self.destinations = destinations
        self.failed_checks = failed_checks
        self.last_successful_check_date = last_successful_check_date
        self.client = sub_manager.client
        for dest in destinations:
            dest.subscription = self

    def to_json(self) -> dict:
        return {
            "link": self.link,
            "path": self.path,
            "last_check_date": self.last_check_date.isoformat(),
            "destinations": [d.to_json() for d in self.destinations],
            "failed_checks": self.failed_checks,
            "last_successful_check_date": self.last_successful_check_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict, sub_manager: "SubscriptionManager") -> "Subscription":
        return cls(
            data["link"],
            data["path"],
            datetime.datetime.fromisoformat(data["last_check_date"]),
            sub_manager,
            [SubscriptionDestination.from_json(d) for d in data["destinations"]],
            data["failed_checks"],
            datetime.datetime.fromisoformat(data["last_successful_check_date"]),
        )

    def matching_chat(self, chat_id: int) -> Optional[SubscriptionDestination]:
        for dest in self.destinations:
            if dest.chat_id == chat_id:
                return dest
        return None

    def matching_dest(self, chat_id: int, user_id: int) -> Optional[SubscriptionDestination]:
        for dest in self.destinations:
            if dest.chat_id == chat_id and dest.creator_id == user_id:
                return dest
        return None

    async def send_new_items(self, new_items: list[str]) -> None:
        for new_item in new_items:
            new_item = convert_if_webp(new_item)
            file_handle = await self.client.upload_file(new_item)
            caption = f"Update on feed: {html.escape(self.link_str)}"
            data_filename = f"{new_item}.json"
            caption_override = self.sub_manager.link_fixer.override_caption(self.link, data_filename)
            if caption_override:
                caption = caption_override
            for dest in self.destinations:
                if dest.paused:
                    continue
                subscription_posts_made.inc()
                await self.client.send_message(
                    entity=dest.chat_id,
                    file=file_handle,
                    message=caption,
                    parse_mode="html",
                )
