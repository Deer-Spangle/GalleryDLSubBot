import asyncio
import datetime
import html
import json
import logging
import os.path
import uuid
from asyncio import Task
from typing import Optional

import aioshutil
from telethon import TelegramClient

from gallery_dl_sub_bot.gallery_dl_manager import GalleryDLManager
from gallery_dl_sub_bot.subscription import (
    Subscription,
    SubscriptionDestination,
    CompleteDownload,
)

logger = logging.getLogger(__name__)


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
        self.complete_downloads = [
            CompleteDownload.from_json(dl_data) for dl_data in config_data.get("downloads", [])
        ]
        self.running = False
        self.runner_task: Optional[Task] = None

    def save(self) -> None:
        config_data = {
            "subscriptions": [s.to_json() for s in self.subscriptions[:]],
            "complete_downloads": [dl.to_json() for dl in self.complete_downloads[:]],
        }
        with open(self.CONFIG_FILE, "w") as f:
            json.dump(config_data, f, indent=2)

    def sub_for_link(self, link: str) -> Optional[Subscription]:
        for sub in self.subscriptions[:]:
            if sub.link == link:
                return sub
        return None

    def sub_for_link_and_chat(self, link: str, chat_id: int) -> Optional[SubscriptionDestination]:
        matching_sub = self.sub_for_link(link)
        if matching_sub is None:
            return None
        return matching_sub.matching_chat(chat_id)

    async def create_download(self, link: str) -> tuple[CompleteDownload, list[str]]:
        # TODO: return a CompleteDownload object
        # TODO: See if a download already exists for this link
        # matching_dl = self.download_for_link(link)
        dl_path = f"store/downloads/{uuid.uuid4()}/"
        # TODO: queueing
        # TODO: in progress message
        lines = await self.dl_manager.download(link, dl_path)
        now = datetime.datetime.now(datetime.timezone.utc)
        dl = CompleteDownload(
            link,
            dl_path,
            now,
        )
        self.complete_downloads.append(dl)
        return dl, lines
        # current_files = matching_dl.list_files()
        # new_files = matching_dl.update(self.dl_manager)

    async def delete_download(self, dl: CompleteDownload) -> None:
        self.complete_downloads.remove(dl)
        await aioshutil.rmtree(dl.path)

    async def create_subscription(self, link: str, chat_id: int, creator_id: int, current_path: str) -> Subscription:
        # See if a subscription already exists for this link
        matching_sub = self.sub_for_link(link)
        # See if that subscription already exists in this chat
        if matching_sub and matching_sub.matching_chat(chat_id):
            raise ValueError("Subscription already exists in this chat for this link")
        # Figure out new path
        new_path = f"store/subscriptions/{uuid.uuid4()}"
        if matching_sub:
            new_path = matching_sub.path
        # Copy files
        if not matching_sub and os.path.exists(current_path):
            await aioshutil.copytree(current_path, new_path)
        # Create destination
        now_date = datetime.datetime.now(datetime.timezone.utc)
        dest = SubscriptionDestination(
            chat_id,
            creator_id,
            now_date,
            False
        )
        # Extend or create subscription
        if matching_sub:
            matching_sub.destinations.append(dest)
            return matching_sub
        sub = Subscription(
            link,
            new_path,
            now_date,
            [dest],
            0,
            now_date,
        )
        self.subscriptions.append(sub)
        self.save()
        return sub

    async def remove_subscription(self, link: str, chat_id: int) -> None:
        found_dest = self.sub_for_link_and_chat(link, chat_id)
        if found_dest is None:
            raise ValueError("Cannot find matching subscription for this link and chat")
        matching_sub = found_dest.subscription
        matching_sub.destinations.remove(found_dest)
        if len(matching_sub.destinations) == 0:
            self.subscriptions.remove(matching_sub)
            await aioshutil.rmtree(matching_sub.path)
        self.save()

    async def pause_subscription(self, link: str, chat_id: int, pause: bool):
        found_dest = self.sub_for_link_and_chat(link, chat_id)
        if found_dest is None:
            raise ValueError("Cannot find matching subscription for this link and chat")
        found_dest.paused = pause
        self.save()

    def start(self) -> None:
        event_loop = asyncio.get_event_loop()
        self.runner_task = event_loop.create_task(self.run())

    async def run(self) -> None:
        self.running = True
        logger.info("Starting subscription manager")
        while self.running:
            for sub in self.subscriptions[:]:
                # Check if subscription needs update
                now = datetime.datetime.now(datetime.timezone.utc)
                if (now - sub.last_check_date) < self.SUB_UPDATE_AFTER:
                    continue
                logger.info("Checking subscription to %s", sub.link)
                # Try and fetch update
                try:
                    sub.last_check_date = now
                    new_items = await self.dl_manager.download(sub.link, sub.path)
                except Exception as e:
                    logger.warning("Failed to check subscription to %s", sub.link, exc_info=e)
                    sub.failed_checks += 1
                    continue
                logger.info("There were %s new items in feed: %s", len(new_items), sub.link)
                # Update timestamps
                now = datetime.datetime.now(datetime.timezone.utc)
                sub.last_check_date = now
                sub.last_successful_check_date = now
                # Send items to destinations
                for new_item in new_items[::-1]:
                    file_handle = await self.client.upload_file(new_item)
                    # media = InputMediaUploadedPhoto(file_handle)
                    for dest in sub.destinations:
                        if dest.paused:
                            continue
                        await self.client.send_message(
                            entity=dest.chat_id,
                            file=file_handle,
                            message=f"Update on feed: {html.escape(sub.link)}",
                            parse_mode="html",
                        )
                self.save()
            await asyncio.sleep(20)

    def stop(self) -> None:
        self.running = False
        loop = asyncio.get_event_loop()
        if self.runner_task and not self.runner_task.done():
            loop.run_until_complete(self.runner_task)
        self.save()

    def list_subscriptions(self, chat_id: int, user_id: int) -> list[SubscriptionDestination]:
        """Lists all the subscriptions matching a given destination and creator, ordered by creation date"""
        sub_dests: list[Optional[SubscriptionDestination]] = [
            sub.matching_dest(chat_id, user_id) for sub in self.subscriptions[:]
        ]
        non_null: list[SubscriptionDestination] = [
            sd for sd in sub_dests if sd is not None
        ]
        sorted_sub_dests = sorted(non_null, key=lambda dest: dest.created_date)
        return sorted_sub_dests
