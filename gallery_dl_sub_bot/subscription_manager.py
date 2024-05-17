import asyncio
import dataclasses
import datetime
import html
import json
import logging
import os.path
import shutil
import uuid
from typing import Optional

from telethon import TelegramClient

from gallery_dl_sub_bot.gallery_dl_manager import GalleryDLManager


logger = logging.getLogger(__name__)

@dataclasses.dataclass
class SubscriptionDestination:
    chat_id: int
    creator_id: int
    created_date: datetime.datetime

    def to_json(self) -> dict:
        return {
            "chat_id": self.chat_id,
            "creator_id": self.creator_id,
            "created_date": self.created_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict) -> "SubscriptionDestination":
        return cls(
            data["chat_id"],
            data["creator_id"],
            datetime.datetime.fromisoformat(data["created_date"]),
        )


@dataclasses.dataclass
class Subscription:
    link: str
    path: str
    destinations: list[SubscriptionDestination]
    last_check_date: datetime.datetime
    failed_checks: int
    last_successful_check_date: datetime.datetime

    def to_json(self) -> dict:
        return {
            "link": self.link,
            "path": self.path,
            "destinations": [d.to_json() for d in self.destinations],
            "last_check_date": self.last_check_date.isoformat(),
            "failed_checks": self.failed_checks,
            "last_successful_check_date": self.last_successful_check_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict) -> "Subscription":
        return cls(
            data["link"],
            data["path"],
            [SubscriptionDestination.from_json(d) for d in data["destinations"]],
            datetime.datetime.fromisoformat(data["last_check_date"]),
            data["failed_checks"],
            datetime.datetime.fromisoformat(data["last_successful_check_date"]),
        )


class SubscriptionManager:
    CONFIG_FILE = "subscriptions.json"
    SUB_UPDATE_AFTER = datetime.timedelta(hours=12)

    def __init__(self, client: TelegramClient, dl_manager: GalleryDLManager) -> None:
        self.client = client
        self.dl_manager = dl_manager
        try:
            with open(self.CONFIG_FILE, "r") as f:
                config_data = json.load(f)
        except FileNotFoundError:
            config_data = {}
        self.subscriptions = [
            Subscription.from_json(sub_data) for sub_data in config_data.get("subscriptions", [])
        ]
        self.running = False

    def save(self) -> None:
        config_data = {
            "subscriptions": [s.to_json() for s in self.subscriptions[:]]
        }
        with open(self.CONFIG_FILE, "w") as f:
            json.dump(config_data, f)

    def sub_for_link(self, link: str) -> Optional[Subscription]:
        for sub in self.subscriptions[:]:
            if sub.link == link:
                return sub
        return None

    async def create_subscription(self, link: str, chat_id: int, creator_id: int, current_path: str) -> Subscription:
        # See if a subscription already exists for this link
        matching_sub = self.sub_for_link(link)
        # Figure out new path
        new_path = f"store/subscriptions/{uuid.uuid4()}"
        if matching_sub:
            new_path = matching_sub.path
        # Copy files
        if not matching_sub and os.path.exists(current_path):
            shutil.copy2(current_path, new_path)
        # Create destination
        now_date = datetime.datetime.now(datetime.timezone.utc)
        dest = SubscriptionDestination(
            chat_id,
            creator_id,
            now_date
        )
        # Extend or create subscription
        if matching_sub:
            matching_sub.destinations.append(dest)
            return matching_sub
        sub = Subscription(
            link,
            new_path,
            [dest],
            now_date,
            0,
            now_date,
        )
        self.subscriptions.append(sub)
        self.save()
        return sub

    async def remove_subscription(self, link: str, chat_id: int) -> None:
        matching_sub = self.sub_for_link(link)
        found_dest = None
        for dest in matching_sub.destinations:
            if dest.chat_id == chat_id:
                found_dest = dest
        if not found_dest:
            raise ValueError("Cannot find matching subscription for this link and chat")
        matching_sub.destinations.remove(found_dest)
        if len(matching_sub.destinations) == 0:
            self.subscriptions.remove(matching_sub)
            shutil.rmtree(matching_sub.path)
        self.save()

    async def start(self) -> None:
        await asyncio.create_task(self.run())

    async def run(self) -> None:
        self.running = True
        while self.running:
            for sub in self.subscriptions[:]:
                # Check if subscription needs update
                now = datetime.datetime.now(datetime.timezone.utc)
                if (now - sub.last_check_date) < self.SUB_UPDATE_AFTER:
                    continue
                # Try and fetch update
                try:
                    sub.last_check_date = now
                    new_items = await self.dl_manager.download(sub.link, sub.path)
                except Exception as e:
                    logger.warning("Failed to check subscription to %s", sub.link, exc_info=e)
                    sub.failed_checks += 1
                    continue
                # Update timestamps
                now = datetime.datetime.now(datetime.timezone.utc)
                sub.last_check_date = now
                sub.last_successful_check_date = now
                # Send items to destinations
                for new_item in new_items[::-1]:
                    file_handle = await self.client.upload_file(new_item)
                    # media = InputMediaUploadedPhoto(file_handle)
                    for dest in sub.destinations:
                        await self.client.send_message(
                            entity=dest.chat_id,
                            file=file_handle,
                            message=f"Update on feed: {html.escape(sub.link)}",
                            parse_mode="html",
                        )
            await asyncio.sleep(20)

    def stop(self) -> None:
        self.running = False
        self.save()
