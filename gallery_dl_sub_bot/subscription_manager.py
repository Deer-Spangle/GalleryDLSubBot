import asyncio
import datetime
import json
import logging
import os
import uuid
from asyncio import Task
from typing import Optional

import aioshutil
from prometheus_client import Gauge, Counter, Histogram
from telethon import TelegramClient

from gallery_dl_sub_bot.gallery_dl_manager import GalleryDLManager
from gallery_dl_sub_bot.link_fixer import LinkFixer
from gallery_dl_sub_bot.subscription import (
    Subscription,
    SubscriptionDestination,
    Download,
    CompleteDownload,
)

logger = logging.getLogger(__name__)

completed_download_count = Gauge(
    "gallerydlsubbot_total_completed_download_count",
    "Total number of completed downloads which have not been subscribed to",
)
subscription_destination_count = Gauge(
    "gallerydlsubbot_total_subscription_destination_count",
    "Total number of subscription-destination pairs at the moment",
)
subscription_destination_count_active = Gauge(
    "gallerydlsubbot_active_subscription_destination_count",
    "Total number of non-paused subscription-destination pairs in the subscription manager at the moment",
)
unique_subscription_count = Gauge(
    "gallerydlsubbot_unique_subscription_count",
    "Total number of unique subscriptions in the subscription manager at the moment",
)
subscription_count_failing = Gauge(
    "gallerydlsubbot_failing_subscription_count",
    "Total number of subscriptions which have failed their most recent check",
)
unique_destination_count = Gauge(
    "gallerydlsubbot_unique_destination_count",
    "Total number of unique destinations which subscriptions are sending to",
)
unique_subscription_creator_count = Gauge(
    "gallerydlsubbot_unique_subscription_creator_count",
    "Total number of unique subscription creators"
)
latest_check_if_updates_needed_time = Gauge(
    "gallerydlsubbot_latest_check_if_updates_needed_unixtime",
    "Timestamp of the latest time the subscription manager checked whether any subscriptions need updating"
)
latest_subscription_checked_time = Gauge(
    "gallerydlsubbot_latest_subscription_checked_unixtime",
    "Timestamp of the last time the subscription manager checked a subscription for updates",
)
subscription_watcher_running = Gauge(
    "gallerydlsubbot_subscription_watcher_running",
    "Whether the subscription watcher is running or not",
)
subscription_check_new_items = Histogram(
    "gallerydlsubbot_subscription_update_size_items",
    "Number of new items found when updating a subscription",
    buckets=[0, 1, 5, 10, 50, 100]
)
subscription_check_time = Histogram(
    "gallerydlsubbot_subscription_update_time_seconds",
    "Amount of time, in seconds, that it took to update a subscription",
    buckets=[1, 5, 60, 300, 600, (30 * 60), (3 * 60 * 60)]
)
subscription_new_items_found = Counter(
    "gallerydlsubbot_subscription_new_items_found_total",
    "Total number of new items found by subscriptions",
)
subscription_total_items_stored = Gauge(
    "gallerydlsubbot_subscription_total_items_stored",
    "Total number of items stored by subscriptions",
)


class SubscriptionManager:
    CONFIG_FILE = "subscriptions.json"
    CONFIG_FILE_BAK = "subscriptions.json.bak"
    SUB_UPDATE_AFTER = datetime.timedelta(hours=5)

    def __init__(self, client: TelegramClient, dl_manager: GalleryDLManager, link_fixer: LinkFixer) -> None:
        self.client = client
        self.dl_manager = dl_manager
        self.link_fixer = link_fixer
        try:
            with open(self.CONFIG_FILE, "r") as f:
                config_data = json.load(f)
        except FileNotFoundError:
            config_data = {}
        self.subscriptions = [
            Subscription.from_json(sub_data, self) for sub_data in config_data.get("subscriptions", [])
        ]
        self.complete_downloads = [
            CompleteDownload.from_json(dl_data, self) for dl_data in config_data.get("complete_downloads", [])
        ]
        self.running = False
        self.runner_task: Optional[Task] = None
        # Metrics
        subscription_watcher_running.set_function(lambda: int(self.running))
        completed_download_count.set_function(lambda: len(self.complete_downloads))
        unique_subscription_count.set_function(lambda: len(self.subscriptions))
        subscription_destination_count.set_function(
            lambda: len([d for s in self.subscriptions for d in s.destinations])
        )
        subscription_destination_count_active.set_function(
            lambda: len([d for s in self.subscriptions for d in s.destinations if not d.paused])
        )
        unique_destination_count.set_function(
            lambda: len(set([d.chat_id for s in self.subscriptions for d in s.destinations]))
        )
        unique_subscription_creator_count.set_function(
            lambda: len(set([d.creator_id for s in self.subscriptions for d in s.destinations]))
        )
        subscription_count_failing.set_function(
            lambda: len([s for s in self.subscriptions if s.failed_checks >= 1])
        )
        subscription_total_items_stored.set_function(lambda: sum(s.num_files for s in self.subscriptions))

    @property
    def all_downloads(self) -> list[Download]:
        return self.subscriptions[:] + self.complete_downloads[:]

    def save(self) -> None:
        config_data = {
            "subscriptions": [s.to_json() for s in self.subscriptions[:]],
            "complete_downloads": [dl.to_json() for dl in self.complete_downloads[:]],
        }
        with open(self.CONFIG_FILE_BAK, "w") as f:
            json.dump(config_data, f, indent=2)
        os.replace(self.CONFIG_FILE_BAK, self.CONFIG_FILE)

    def download_for_link(self, link: str) -> Optional[Download]:
        for download in self.all_downloads:
            if download.link == link or download.link_str == link:
                return download
        return None

    def sub_for_link(self, link: str) -> Optional[Subscription]:
        for sub in self.subscriptions[:]:
            if sub.link == link or sub.link_str == link:
                return sub
        return None

    def sub_for_link_and_chat(self, link: str, chat_id: int) -> Optional[SubscriptionDestination]:
        matching_sub = self.sub_for_link(link)
        if matching_sub is None:
            return None
        return matching_sub.matching_chat(chat_id)

    async def create_download(self, link: str) -> Download:
        # See if a download already exists for this link
        matching_dl = self.download_for_link(link)
        if matching_dl:
            return matching_dl
        dl_path = f"store/downloads/{uuid.uuid4()}/"
        now = datetime.datetime.now(datetime.timezone.utc)
        dl = CompleteDownload(
            link, dl_path, now, self
        )
        self.complete_downloads.append(dl)
        self.save()
        return dl

    async def delete_download(self, dl: Download) -> None:
        if isinstance(dl, CompleteDownload):
            self.complete_downloads.remove(dl)
            if dl.active_download and False:
                dl.active_download.kill()
            async with dl.zip_lock:
                await aioshutil.rmtree(dl.path)

    async def create_subscription(self, chat_id: int, creator_id: int, current_dl: Download) -> Subscription:
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
            dest.subscription = current_dl
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
            current_dl.link,
            new_path,
            now_date,
            self,
            [dest],
            0,
            now_date,
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
            try:
                await self._check_for_updates()
                await self._sleep(20)
            except Exception as e:
                logger.critical("Subscription watcher has failed: ", exc_info=e)
                self.running = False

    async def _check_for_updates(self) -> None:
        logger.info("Checking which subscriptions need update")
        for sub in self.subscriptions[:]
            if not self.running:
                return
            latest_check_if_updates_needed_time.set_to_current_time()
            # Check if subscription needs update
            now = datetime.datetime.now(datetime.timezone.utc)
            if (now - sub.last_check_date) < self.SUB_UPDATE_AFTER:
                continue
            logger.info("Checking subscription to %s", sub.link)
            latest_subscription_checked_time.set_to_current_time()
            # Try and fetch update
            new_items = []
            zero_batches = 0
            try:
                with subscription_check_time.time():
                    async for line_batch in sub.download():
                        new_items += line_batch
                        if len(line_batch) == 0:
                            zero_batches += 1
                            if zero_batches > 10:
                                logger.warning(
                                    "Got 10 empty batches in a row for subscription %s, stopping here",
                                    sub.link
                                )
                                break
                        logger.info("Got new batch of %s lines in %s check", len(line_batch), sub.link)
            except Exception as e:
                logger.warning("Failed to check subscription to %s", sub.link, exc_info=e)
                sub.failed_checks += 1
                sub.last_check_date = datetime.datetime.now(datetime.timezone.utc)
                self.save()
                continue
            logger.info("In total there are %s items in feed: %s", len(new_items), sub.link)
            if sub.active_download:
                logger.info("There were %s new items in feed: %s", len(sub.active_download.lines_so_far), sub.link)
                subscription_check_new_items.observe(len(sub.active_download.lines_so_far))
                subscription_new_items_found.inc(len(sub.active_download.lines_so_far))
            # Update timestamps
            now = datetime.datetime.now(datetime.timezone.utc)
            sub.last_check_date = now
            sub.last_successful_check_date = now
            self.save()

    async def _sleep(self, seconds: float) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        end = now + datetime.timedelta(seconds=seconds)
        while self.running and now < end:
            await asyncio.sleep(0.5)
            now = datetime.datetime.now(datetime.timezone.utc)

    def stop(self) -> None:
        self.running = False
        loop = asyncio.get_event_loop()
        if self.runner_task and not self.runner_task.done():
            loop.run_until_complete(self.runner_task)
        # Kill all downloads in progress
        for dl in self.all_downloads:
            if dl.active_download is not None and not dl.active_download.complete:
                dl.active_download.kill()
        # Save config
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
