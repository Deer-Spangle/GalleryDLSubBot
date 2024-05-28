import asyncio
import datetime
import json
import logging
import uuid
from asyncio import Task
from contextlib import asynccontextmanager
from typing import Optional, AsyncIterator

import aioshutil
from telethon import TelegramClient

from gallery_dl_sub_bot.gallery_dl_manager import GalleryDLManager
from gallery_dl_sub_bot.subscription import (
    Subscription,
    SubscriptionDestination,
    Download,
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
            Subscription.from_json(sub_data, dl_manager, client) for sub_data in config_data.get("subscriptions", [])
        ]
        self.complete_downloads = [
            CompleteDownload.from_json(dl_data, dl_manager) for dl_data in config_data.get("downloads", [])
        ]
        self.running = False
        self.runner_task: Optional[Task] = None

    @property
    def all_downloads(self) -> list[Download]:
        return self.subscriptions[:] + self.complete_downloads[:]  # TODO: + self.dl_in_progress[:]

    def save(self) -> None:
        config_data = {
            "subscriptions": [s.to_json() for s in self.subscriptions[:]],
            "complete_downloads": [dl.to_json() for dl in self.complete_downloads[:]],
        }
        with open(self.CONFIG_FILE, "w") as f:
            json.dump(config_data, f, indent=2)

    def download_for_link(self, link: str) -> Optional[Download]:
        for download in self.all_downloads:
            if download.link == link:
                return download
        return None

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

    async def create_download(self, link: str) -> tuple[Download, list[str]]:
        # See if a download already exists for this link
        matching_dl = self.download_for_link(link)
        if matching_dl:
            dl_path = matching_dl.path
            existing_files = matching_dl.list_files()
            new_lines = await self.dl_manager.download(link, dl_path)
            await matching_dl.send_new_items(new_lines[::-1])
            now = datetime.datetime.now(datetime.timezone.utc)
            matching_dl.last_check_date = now
            self.save()
            return matching_dl, existing_files + new_lines[::-1]
        dl_path = f"store/downloads/{uuid.uuid4()}/"
        # TODO: queueing
        # TODO: in progress message
        lines = await self.dl_manager.download(link, dl_path)
        now = datetime.datetime.now(datetime.timezone.utc)
        dl = CompleteDownload(
            link,
            dl_path,
            now,
            self.dl_manager,
        )
        self.complete_downloads.append(dl)
        self.save()
        return dl, lines
        # current_files = matching_dl.list_files()
        # new_files = matching_dl.update(self.dl_manager)

    async def delete_download(self, dl: Download) -> None:
        if isinstance(dl, CompleteDownload):
            self.complete_downloads.remove(dl)
            async with dl.zip_lock:
                await aioshutil.rmtree(dl.path)

    async def create_subscription(self, link: str, chat_id: int, creator_id: int, current_dl: Download) -> Subscription:
        # Create destination
        now_date = datetime.datetime.now(datetime.timezone.utc)
        dest = SubscriptionDestination(
            chat_id,
            creator_id,
            now_date,
            False
        )
        # If current download is a subscription, just add a new destination
        if isinstance(current_dl, Subscription):
            # See if that subscription already exists in this chat
            if current_dl.matching_chat(chat_id):
                raise ValueError("Subscription already exists in this chat for this link")
            # Extend existing subscription
            current_dl.destinations.append(dest)
            self.save()
            return current_dl
        # If not a CompleteDownload, raise exception
        if not isinstance(current_dl, CompleteDownload):
            raise ValueError("Download is not complete")  # TODO: wait for complete
        # Figure out new path
        new_path = f"store/subscriptions/{uuid.uuid4()}"
        # Copy files to new path
        await aioshutil.copytree(current_dl.path, new_path)
        # Otherwise create new subscription
        sub = Subscription(
            link,
            new_path,
            now_date,
            self.dl_manager,
            [dest],
            0,
            now_date,
            self.client,
        )
        # Add new subscription, remove download
        self.subscriptions.append(sub)
        # Delete download
        await self.delete_download(current_dl)
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
            async with matching_sub.zip_lock:
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
                await sub.send_new_items(new_items[::-1])
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

    @asynccontextmanager
    async def create_zip(self, dl: Download, filename: str) -> AsyncIterator[str]:
        zip_dir = f"store/zips/{uuid.uuid4()}"
        zip_path = f"{zip_dir}/{filename}"
        async with dl.zip_lock:
            try:
                await aioshutil.make_archive(zip_path, "zip", dl.path)
                yield f"{zip_path}.zip"
            finally:
                await aioshutil.rmtree(zip_dir)
