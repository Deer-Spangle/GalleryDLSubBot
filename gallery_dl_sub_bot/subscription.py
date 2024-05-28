import asyncio
import dataclasses
import datetime
import glob
import html
from typing import Optional

from telethon import TelegramClient



class Download:

    def __init__(self, link: str, path: str, last_check_date: datetime.datetime) -> None:
        self.link = link
        self.path = path
        self.last_check_date = last_check_date
        self.zip_lock = asyncio.Lock()

    def list_files(self) -> list[str]:
        all_files = glob.glob(self.path + '/**/*.*', recursive=True)
        img_files = [f for f in all_files if not (f.endswith(".json") or f.endswith(".sqlite"))]
        return sorted(img_files)

    async def send_new_items(self, new_items: list[str], client: TelegramClient) -> None:
        pass


class CompleteDownload(Download):

    def to_json(self) -> dict:
        return {
            "link": self.link,
            "path": self.path,
            "last_check_date": self.last_check_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict) -> "CompleteDownload":
        return cls(
            data["link"],
            data["path"],
            datetime.datetime.fromisoformat(data["last_check_date"]),
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
            link: str,
            path: str,
            last_check_date: datetime.datetime,
            destinations: list[SubscriptionDestination],
            failed_checks: int,
            last_successful_check_date: datetime.datetime,
    ) -> None:
        super().__init__(link, path, last_check_date)
        self.destinations = destinations
        self.failed_checks = failed_checks
        self.last_successful_check_date = last_successful_check_date

    def __post_init__(self):
        for dest in self.destinations:
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
    def from_json(cls, data: dict) -> "Subscription":
        return cls(
            data["link"],
            data["path"],
            datetime.datetime.fromisoformat(data["last_check_date"]),
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

    async def send_new_items(self, new_items: list[str], client: TelegramClient) -> None:
        for new_item in new_items:
            file_handle = await client.upload_file(new_item)
            for dest in self.destinations:
                if dest.paused:
                    continue
                await client.send_message(
                    entity=dest.chat_id,
                    file=file_handle,
                    message=f"Update on feed: {html.escape(self.link)}",
                    parse_mode="html",
                )
