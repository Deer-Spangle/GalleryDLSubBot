import asyncio
import datetime
import html
import json
import logging
import re
import shlex
from typing import Optional, Callable, Awaitable

import telethon
from prometheus_client import Gauge, start_http_server, Counter, Histogram
from telethon import TelegramClient, events, Button

from gallery_dl_sub_bot.auth_manager import AuthManager
from gallery_dl_sub_bot.date_format import format_last_check
from gallery_dl_sub_bot.gallery_dl_manager import GalleryDLManager
from gallery_dl_sub_bot.hidden_data import parse_hidden_data, hidden_data
from gallery_dl_sub_bot.link_fixer import LinkFixer, link_to_str
from gallery_dl_sub_bot.subscription import SubscriptionDestination, Download
from gallery_dl_sub_bot.subscription_manager import SubscriptionManager

logger = logging.getLogger(__name__)

PROM_PORT = 7168
start_time = Gauge("gallerydlsubbot_start_unixtime", "Unix timestamp of the last time the bot was started")
function_usage_count = Counter(
    "gallerydlsubbot_function_usage_count",
    "Count of how many times different functions of the bot have been used",
    labelnames=["function"],
)
boop_usage_count = function_usage_count.labels(function="Boop")
start_usage_count = function_usage_count.labels(function="Start menu")
subscription_menu_summon_count = function_usage_count.labels(function="Summon subscription menu")
gallery_dl_update_menu_summon_count = function_usage_count.labels(function="Summon update menu")
raw_download_usage_count = function_usage_count.labels(function="Raw download request")
unknown_command_usage_count = function_usage_count.labels(function="Unknown command")
embed_request_count = function_usage_count.labels(function="Embed request")
zip_request_count = function_usage_count.labels(function="Zip request")
subscribe_request_count = function_usage_count.labels(function="Subscription request")
unsubscribe_request_count = function_usage_count.labels(function="Unsubscribe request")
pause_request_count = function_usage_count.labels(function="Pause subscription")
unpause_request_count = function_usage_count.labels(function="Resume subscription")
url_request_message_count = function_usage_count.labels(function="URL request")
failed_auth_attempts = Counter(
    "gallerydlsubbot_failed_auth_attempt_count",
    "Number of times someone has been denied auth for an action they attempted to do",
)
url_request_url_count = Counter(
    "gallerydlsubbot_url_request_url_count",
    "Number of URLs which have been sent to the bot to download",
)
initial_download_size = Histogram(
    "gallerydlsubbot_initial_download_size_items",
    "Number of items in the initial download of a URL",
    buckets=[0, 1, 5, 10, 50, 100, 500, 1000, 10_000]
)
initial_download_time = Histogram(
    "gallerydlsubbot_initial_download_time_seconds",
    "Amount of time, in seconds, that the initial download of a URL took",
    buckets=[1, 5, 60, 300, 600, (30 * 60), (3 * 60 * 60)]
)


async def _check_sender(evt: events.CallbackQuery.Event, allowed_user_id: int) -> None:
    if evt.sender_id != allowed_user_id:
        await evt.answer("Unauthorized menu use")
        raise events.StopPropagation


class Bot:
    SUBS_PER_MENU_PAGE = 10
    MAX_ALBUM_SIZE = 10
    MAX_OFFER_EMBED = 100

    def __init__(self, config: dict) -> None:
        self.config = config
        session_name = "gallery_dl_sub_bot"
        if suffix := self.config["telegram"].get("session_suffix"):
            session_name += f"__{suffix}"
        self.client = TelegramClient(
            session_name, self.config["telegram"]["api_id"], self.config["telegram"]["api_hash"]
        )
        self.dl_manager = GalleryDLManager("config_gallery_dl.json")
        self.auth_manager = AuthManager("trusted_users.yaml")
        self.link_fixer = LinkFixer()
        self.sub_manager = SubscriptionManager(self.client, self.dl_manager, self.link_fixer)

    def run(self) -> None:
        start_time.set_to_current_time()
        self.client.start(bot_token=self.config["telegram"]["bot_token"])
        # Register functions
        self.client.add_event_handler(self.start, events.NewMessage(pattern="/start", incoming=True))
        self.client.add_event_handler(self.boop, events.NewMessage(pattern="/beep", incoming=True))
        self.client.add_event_handler(
            self.summon_subscription_menu,
            events.NewMessage(pattern="/subscriptions", incoming=True),
        )
        self.client.add_event_handler(
            self.update_gallery_dl,
            events.NewMessage(pattern="/update_gallery_dl", incoming=True)
        )
        self.client.add_event_handler(self.raw_download, events.NewMessage(pattern="/raw", incoming=True))
        self.client.add_event_handler(self.unknown_command, events.NewMessage(pattern="/", incoming=True))
        self.client.add_event_handler(self.check_for_links, events.NewMessage(incoming=True))
        self.client.add_event_handler(self.handle_embed_callback, events.CallbackQuery(pattern="embed:"))
        self.client.add_event_handler(self.handle_zip_callback, events.CallbackQuery(pattern="dl_zip:"))
        self.client.add_event_handler(self.handle_subscribe_callback, events.CallbackQuery(pattern="subscribe:"))
        self.client.add_event_handler(self.page_subscriptions_menu, events.CallbackQuery(pattern="subs_offset:"))
        self.client.add_event_handler(self.view_subscription_menu, events.CallbackQuery(pattern="subs_menu:"))
        self.client.add_event_handler(self.handle_unsubscribe_callback, events.CallbackQuery(pattern="unsubscribe:"))
        self.client.add_event_handler(self.handle_pause_callback, events.CallbackQuery(pattern="pause:"))
        self.client.add_event_handler(self.handle_update_callback, events.CallbackQuery(pattern="update:"))
        # Start prometheus server
        start_http_server(PROM_PORT)
        # Start listening
        try:
            # Start subscription manager
            self.sub_manager.start()
            # Start bot listening
            logger.info("Starting bot")
            self.client.run_until_disconnected()
        finally:
            self.sub_manager.stop()
            logger.info("Bot sleepy bye-bye time")

    # noinspection PyMethodMayBeStatic
    async def boop(self, event: events.NewMessage.Event) -> None:
        boop_usage_count.inc()
        await event.reply("Boop!")
        raise events.StopPropagation

    # noinspection PyMethodMayBeStatic
    async def start(self, event: events.NewMessage.Event) -> None:
        start_usage_count.inc()
        await event.reply("Hey there! I'm not a very good bot yet, I'm quite early in development.")
        raise events.StopPropagation

    async def check_for_links(self, event: events.NewMessage.Event) -> None:
        logger.info("Got a message from user %s", event.sender_id)
        if not self.auth_manager.user_is_trusted(event.sender_id):
            failed_auth_attempts.inc()
            logger.info("Unauthorised user has sent a msg")
            await event.reply("Apologies, you are not authorised to operate this bot")
            raise events.StopPropagation
        link_regex = re.compile(r"(https?://|www\.|[^\s/]+\.com)[^\s'\"()[\]]+", re.I)
        links = []
        # Find links in text
        for link in link_regex.finditer(event.message.text):
            links.append(link.group(0))
        # Find links in buttons
        if event.message.buttons:
            for button_row in event.message.buttons:
                for button in button_row:
                    if button.url:
                        links.append(button.url)
        if not links:
            await event.reply("Could not find any links in that message")
            raise events.StopPropagation
        # Fix all the links
        fixed_links = []
        for link in links:
            fixed_link = self.link_fixer.fix_link(link)
            if fixed_link not in fixed_links:
                fixed_links.append(fixed_link)
        # Increment metrics
        url_request_message_count.inc()
        url_request_url_count.inc(len(fixed_links))
        # Tell the user all the links
        if len(fixed_links) > 1:
            lines = [f"- {html.escape(link)}" for link in fixed_links]
            await event.reply("Found these links:\n" + "\n".join(lines), parse_mode="html", link_preview=False)
        # Check them in gallery-dl
        await asyncio.gather(*(self._handle_link(link, event) for link in fixed_links))
        raise events.StopPropagation

    async def _handle_link(
            self,
            link: str | list[str],
            event: events.NewMessage.Event,
            allow_auto_embed: bool = True,
    ) -> None:
        dl, lines = await self._download_link(link, event)
        link_str = link_to_str(link)
        # If no images, stop now
        if len(lines) == 0:
            return
        # If less than 10 things, just post an album
        if allow_auto_embed and len(lines) <= self.MAX_ALBUM_SIZE:
            await self._post_album(event, lines, link)
            await self.sub_manager.delete_download(dl)
            return
        # Otherwise post menus
        hidden_link = hidden_data({
            "link": link_str,
            "user_id": str(event.sender_id),
        })
        # If it's not too many, offer to send anyway
        if len(lines) <= self.MAX_OFFER_EMBED:
            offer_embed_msg = f"Would you like me to send them as Telegram albums anyway?{hidden_link}"
            if not allow_auto_embed:
                offer_embed_msg = f"Would you like me to send them as Telegram albums?{hidden_link}"
            await event.reply(
                offer_embed_msg,
                parse_mode="html",
                buttons=[[
                    Button.inline("Yes", "embed:yes"),
                    Button.inline("No thanks", "embed:no")
                ]]
            )
        # Offer to zip up the feed
        await event.reply(
            f"Would you like to download these files as a zip?{hidden_link}",
            parse_mode="html",
            buttons=[[
                Button.inline("Yes", "dl_zip:yes"),
                Button.inline("No thanks", "dl_zip:no"),
            ]]
        )
        # Unless already subscribed, offer to subscribe
        if self.sub_manager.sub_for_link_and_chat(link_str, event.chat_id):
            await event.reply(
                f"You are already subscribed to {html.escape(link_str)} in this chat.",
                parse_mode="html",
                link_preview=False,
            )
        else:
            await event.reply(
                f"Would you like to subscribe to {html.escape(link_str)}?{hidden_link}",
                parse_mode="html",
                buttons=[[
                    Button.inline("Yes, subscribe", "subscribe:yes"),
                    Button.inline("No thanks", "subscribe:no"),
                ]],
                link_preview=False,
            )

    async def _download_link(
            self,
            link: str | list[str],
            request_evt: events.NewMessage.Event,
    ) -> tuple[Download, list[str]]:
        link_str = link_to_str(link)
        evt = await request_evt.reply(
            f"⏳ Downloading link: {html.escape(link_str)}",
            parse_mode="html",
            link_preview=False,
        )
        lines = []
        last_progress_update = datetime.datetime.now(datetime.timezone.utc)
        last_line_count: Optional[int] = None
        with initial_download_time.time():
            try:
                dl = await self.sub_manager.create_download(link)
                async for lines_batch in dl.download():
                    lines += lines_batch
                    now = datetime.datetime.now(datetime.timezone.utc)
                    line_count = len(lines)
                    if (now - last_progress_update) < datetime.timedelta(seconds=10) or line_count == last_line_count:
                        continue
                    await evt.edit(
                        f"⏳ Downloading link: {html.escape(link_str)}\n(Found {line_count} images so far...)",
                        parse_mode="html",
                        link_preview=False,
                    )
                    last_progress_update = datetime.datetime.now(datetime.timezone.utc)
                    last_line_count = line_count
            except Exception as e:
                logger.error(f"Failed to download link {link}", exc_info=e)
                await request_evt.reply(f"Failed to download link {html.escape(link_str)} :(")
                await evt.delete()
                raise e
        # Post update on feed size
        initial_download_size.observe(len(lines))
        await request_evt.reply(f"Found {len(lines)} images(s) in link: {html.escape(link_str)}", parse_mode="html")
        await evt.delete()
        return dl, lines

    async def _post_album(self, event: events.NewMessage.Event, lines: list[str], link: str) -> None:
        caption = f"{html.escape(link_to_str(link))}"
        # Check for caption override
        data_file = f"{lines[0]}.json"
        caption_override = self.link_fixer.override_caption(link, data_file)
        if caption_override:
            caption = caption_override
        # Post the album
        await event.reply(caption, parse_mode="html", file=lines)
        return

    async def handle_embed_callback(self, event: events.CallbackQuery.Event) -> None:
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"embed:")
        logger.info(f"Callback query pressed: {query_data}")
        menu_msg = await event.get_message()
        # Parse menu data
        menu_data = parse_hidden_data(menu_msg)
        link = menu_data["link"]
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Find the matching Download
        dl = self.sub_manager.download_for_link(link)
        if dl is None:
            await menu_msg.edit("Error: This download seems to have disappeared", buttons=None)
            raise events.StopPropagation
        # Handle no button
        if query_resp == b"no":
            await menu_msg.delete()
            raise events.StopPropagation
        # Handle yes button
        if query_resp == b"yes":
            embed_request_count.inc()
            lines = dl.list_files()
            link_msg = await menu_msg.get_reply_message()
            for start in range(0, len(lines), self.MAX_ALBUM_SIZE):
                lines_chunk = lines[start:start + self.MAX_ALBUM_SIZE]
                await self._post_album(link_msg, lines_chunk, link)
                await menu_msg.delete()
            raise events.StopPropagation
        # Handle other callback data
        await event.answer("Unrecognised response")

    async def handle_zip_callback(self, event: events.CallbackQuery.Event) -> None:
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"dl_zip:")
        logger.info(f"Callback query pressed: {query_data}")
        menu_msg = await event.get_message()
        # Parse menu data
        menu_data = parse_hidden_data(menu_msg)
        link = menu_data["link"]
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Find the matching Download
        dl = self.sub_manager.download_for_link(link)
        if dl is None:
            await menu_msg.edit("Error: This download seems to have disappeared", buttons=None)
            raise events.StopPropagation
        # Handle no button
        if query_resp == b"no":
            await menu_msg.delete()
            raise events.StopPropagation
        # Handle yes button
        if query_resp == b"yes":
            zip_request_count.inc()
            await menu_msg.edit("⏳ Creating zip archive...", buttons=None)
            zip_filename = self.link_fixer.link_to_filename(link)
            async with dl.zip(zip_filename) as zip_files:
                link_msg = await menu_msg.get_reply_message()
                if len(zip_files) == 1:
                    await link_msg.reply(
                        f"Here is the zip archive of {html.escape(link)}",
                        file=zip_files[0],
                        parse_mode="html",
                        link_preview=False,
                    )
                    await menu_msg.delete()
                else:
                    zip_count = len(zip_files)
                    await link_msg.reply(
                        f"Due to telegram size limits, zip archive was split into {zip_count} parts.\n"
                        f"Here is part 1/{zip_count} of the zip archive of {html.escape(link)}\n"
                        "Please download all parts before attempting to unzip the archive",
                        file=zip_files[0],
                        parse_mode="html",
                        link_preview=False,
                    )
                    for n, zip_file in enumerate(zip_files[1:], start=2):
                        await link_msg.reply(
                            f"Here is part {n}/{zip_count} of the zip archive of {html.escape(link)}",
                            file=zip_file,
                            parse_mode="html",
                            link_preview=False,
                        )
                    await menu_msg.delete()
            raise events.StopPropagation
        # Handle other callback data
        await event.answer("Unrecognised response")

    async def handle_subscribe_callback(self, event: events.CallbackQuery.Event) -> None:
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"subscribe:")
        logger.info(f"Callback query pressed: {query_data}")
        menu_msg = await event.get_message()
        # Parse menu data
        menu_data = parse_hidden_data(menu_msg)
        link = menu_data["link"]
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Find matching Download
        dl = self.sub_manager.download_for_link(link)
        if dl is None:
            await menu_msg.edit("Error: This download seems to have disappeared", buttons=None)
            raise events.StopPropagation
        # Handle no button
        if query_resp == b"no":
            await menu_msg.delete()
            raise events.StopPropagation
        # Handle yes button press
        if query_resp == b"yes":
            subscribe_request_count.inc()
            await menu_msg.edit("⏳ Subscribing...", buttons=None)
            try:
                await self.sub_manager.create_subscription(menu_msg.chat_id, user_id, dl)
            except Exception as e:
                logger.error(f"Failed to subscribe to {link}", exc_info=e)
                await menu_msg.edit(
                    f"Failed to create subscription to {html.escape(link)}",
                    parse_mode="html",
                    link_preview=False,
                    buttons=None,
                )
                raise e
            link_msg = await menu_msg.get_reply_message()
            await link_msg.reply(f"Subscription created for {html.escape(link)}", parse_mode="html", link_preview=False)
            await menu_msg.delete()
            raise events.StopPropagation
        # Handle other callback data
        await event.answer("Unrecognised response")

    async def summon_subscription_menu(self, event: events.NewMessage.Event) -> None:
        subscription_menu_summon_count.inc()
        chat_id = event.chat_id
        user_id = event.sender_id
        sub_dests = self.sub_manager.list_subscriptions(chat_id, user_id)
        if len(sub_dests) == 0:
            await event.reply("You have no subscriptions in this chat. Send a link to create one")
            raise events.StopPropagation
        async def post_cmd(text: str, buttons: Optional[list[list[Button]]]) -> None:
            await event.reply(
                text,
                parse_mode="html",
                link_preview=False,
                buttons=buttons,
            )
        await self._post_subscription_menu(post_cmd, sub_dests, 0, user_id)
        raise events.StopPropagation

    async def _post_subscription_menu(
            self,
            post_cmd: Callable[[str, Optional[list[list[Button]]]], Awaitable[None]],
            sub_dests: list[SubscriptionDestination],
            offset: int,
            user_id: int,
    ):
        page_size = self.SUBS_PER_MENU_PAGE
        while page_size > 0:
            try:
                await post_cmd(
                    self._list_subscriptions_menu_text(sub_dests, page_size, offset, user_id),
                    self._list_subscriptions_menu_buttons(sub_dests, page_size, offset),
                )
                return
            except telethon.errors.rpcerrorlist.MessageTooLongError:
                page_size -= 1
                logger.warning("Subscription menu too long to post, changing page size down to %s", page_size)
        logger.error("Completely failed to post subscription menu, single item too long")
        await post_cmd(
            "I can't post your subscription list, as an item is too long to fit in a Telegram message",
            None,
        )

    # noinspection PyMethodMayBeStatic
    def _list_subscriptions_menu_buttons(
            self,
            subs: list[SubscriptionDestination],
            page_size: int,
            offset: int,
    ) -> list[list[Button]]:
        # Cap offset
        if offset < 0:
            offset = 0
        if offset >= len(subs):
            offset = len(subs) - 1
        # Get the page's subscription list
        subs_page = subs[offset:offset+page_size]
        # Construct the pagination buttons
        has_prev = offset > 0
        has_next = len(subs) > page_size + offset
        pagination_row = []
        if has_prev:
            prev_offset = max(offset-page_size, 0)
            pagination_row.append(Button.inline("⬅️Prev", f"subs_offset:{prev_offset}"))
        if has_next:
            next_offset = offset + page_size
            pagination_row.append(Button.inline("➡️Next", f"subs_offset:{next_offset}"))
        # Construct button list
        return [
                [Button.inline(f"{n}) {sub.subscription.link_str}", f"subs_menu:{n}")]
                for n, sub in enumerate(subs_page, start=1+offset)
            ] + [
            pagination_row
        ]

    # noinspection PyMethodMayBeStatic
    def _list_subscriptions_menu_text(
            self,
            subs: list[SubscriptionDestination],
            page_size: int,
            offset: int,
            user_id: int,
    ) -> str:
        menu_data = hidden_data({"offset": str(offset), "user_id": str(user_id)})
        menu_text = f"{menu_data}You have {len(subs)} subscriptions in this chat:\n"
        lines = []
        for n, sub in enumerate(subs, start=1):
            bpt = "-"
            idx = n - 1
            if offset <= idx < offset + page_size:
                bpt = "*"
            suffix = ""
            if sub.subscription.failed_checks > 0:
                suffix = " (failing checks)"
            if sub.paused:
                suffix = " (paused)"
            lines.append(f"{bpt} {n}) {html.escape(sub.subscription.link_str)}{suffix}")
        menu_text += "\n".join(lines)
        return menu_text

    async def page_subscriptions_menu(self, event: events.CallbackQuery.Event) -> None:
        # Parse callback data
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"subs_offset:")
        offset = int(query_resp)
        # Parse menu data
        menu_msg = await event.get_message()
        menu_data = parse_hidden_data(menu_msg)
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Get subscription list
        chat_id = event.chat_id
        sub_dests = self.sub_manager.list_subscriptions(chat_id, user_id)
        # Handle empty subscription list
        if len(sub_dests) == 0:
            await menu_msg.edit("You have no subscriptions in this chat. Send a link to create one")
            raise events.StopPropagation
        # Send menu
        async def post_cmd(text: str, buttons: Optional[list[list[Button]]]) -> None:
            await menu_msg.edit(
                text,
                parse_mode="html",
                link_preview=False,
                buttons=buttons,
            )
        await self._post_subscription_menu(post_cmd, sub_dests, offset, user_id)
        raise events.StopPropagation

    async def view_subscription_menu(self, event: events.CallbackQuery.Event) -> None:
        # Parse callback data
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"subs_menu:")
        view_sub_idx = int(query_resp) - 1
        # Parse menu data
        menu_msg = await event.get_message()
        menu_data = parse_hidden_data(menu_msg)
        offset = int(menu_data["offset"])
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Get subscription list
        chat_id = event.chat_id
        sub_dests = self.sub_manager.list_subscriptions(chat_id, user_id)
        # Handle empty subscription list
        if len(sub_dests) == 0:
            await menu_msg.edit("You have no subscriptions in this chat. Send a link to create one")
            raise events.StopPropagation
        # Check subscription index is valid
        if 0 > view_sub_idx or len(sub_dests) <= view_sub_idx:
            await event.answer("Subscription index not valid")
            async def post_cmd(text: str, buttons: Optional[list[list[Button]]]) -> None:
                await menu_msg.edit(
                    text,
                    parse_mode="html",
                    link_preview=False,
                    buttons=buttons,
                )

            await self._post_subscription_menu(post_cmd, sub_dests, offset, user_id)
            raise events.StopPropagation
        # Get subscription and destination
        sub_dest = sub_dests[view_sub_idx]
        sub = sub_dest.subscription
        # Assemble menu data
        msg_data = {
            "link": sub.link_str,
            "user_id": user_id,
        }
        # Send menu
        view_sub_lines = [f"{hidden_data(msg_data)}Viewing subscription: {html.escape(sub.link_str)}"]
        view_sub_lines += [f"Created: {format_last_check(sub_dest.created_date)}"]
        if sub.failed_checks > 0:
            view_sub_lines += [f"Failed last {sub.failed_checks} checks"]
            view_sub_lines += [f"Last successful check was: {format_last_check(sub.last_successful_check_date)}"]
        pause_button = "Pause subscription"
        pause_callback = "pause:pause"
        if sub_dest.paused:
            view_sub_lines += ["Subscription is paused."]
            pause_button = "Resume subscription"
            pause_callback = "pause:resume"
        await menu_msg.edit(
            "\n".join(view_sub_lines),
            parse_mode="html",
            link_preview=False,
            buttons=[
                [Button.inline("Download zip", "dl_zip:yes")],
                [Button.inline(pause_button, pause_callback)],
                [Button.inline("Unsubscribe", "unsubscribe:yes")],
                [Button.inline("⬅️Back to list", f"subs_offset:{offset}")]
            ],
        )
        raise events.StopPropagation

    async def handle_unsubscribe_callback(self, event: events.CallbackQuery.Event) -> None:
        # Parse callback data
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"unsubscribe:")
        if query_resp != b"yes":
            await event.answer("Unrecognised unsubscribe command")
            raise events.StopPropagation
        # Parse menu data
        menu_msg = await event.get_message()
        menu_data = parse_hidden_data(menu_msg)
        link = menu_data["link"]
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Unsubscribe
        unsubscribe_request_count.inc()
        await menu_msg.edit(
            f"⏳ Unsubscribing from {html.escape(link)}...",
            parse_mode="html",
            link_preview=False,
            buttons=None,
        )
        chat_id = event.chat_id
        await self.sub_manager.remove_subscription(link, chat_id)
        await menu_msg.edit(
            f"Unsubscribed from {html.escape(link)}",
            parse_mode="html",
            link_preview=False,
            buttons=None,
        )
        raise events.StopPropagation

    async def handle_pause_callback(self, event: events.CallbackQuery.Event) -> None:
        # Parse callback data
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"pause:")
        # Parse menu data
        menu_msg = await event.get_message()
        menu_data = parse_hidden_data(menu_msg)
        link_str = menu_data["link"]
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Check callback data
        if query_resp == b"pause":
            pause_request_count.inc()
            pause_sub = True
        elif query_resp == b"resume":
            unpause_request_count.inc()
            pause_sub = False
        else:
            await event.answer("Unrecognised pause callback")
            raise events.StopPropagation
        # Pause subscription
        chat_id = event.chat_id
        await self.sub_manager.pause_subscription(link_str, chat_id, pause_sub)
        # Refresh menu menu
        pause_verb = "Paused" if pause_sub else "Resumed"
        await menu_msg.edit(
            f"{pause_verb} subscription to {html.escape(link_str)}",
            parse_mode="html",
            link_preview=False,
            buttons=None,
        )

    # noinspection PyMethodMayBeStatic
    def _gallery_dl_version_text(self, version: str, install_type: str, last_update: datetime.datetime) -> str:
        return (
            f"Gallery-dl is currently on a {install_type} install, v{version}.\n"
            f"Last update: {format_last_check(last_update)}"
        )

    async def update_gallery_dl(self, event: events.NewMessage.Event) -> None:
        gallery_dl_update_menu_summon_count.inc()
        user_id = event.sender_id
        logger.info("Request to update gallery-dl from %s", user_id)
        # Check if they are authorised to use the bot
        if not self.auth_manager.user_is_trusted(user_id):
            failed_auth_attempts.inc()
            logger.info("Unauthorised user has tried to update gallery-dl: %s", user_id)
            await event.reply("Apologies, you are not authorised to operate this bot")
            raise events.StopPropagation
        # Check it is installed
        if self.dl_manager.last_update is None:
            inp = await event.reply("⏳ Checking gallery-dl install", parse_mode="html")
            await self.dl_manager.check_install()
            await inp.delete()
        # Fetch current version
        version = await self.dl_manager.get_tool_version()
        install_type = self.dl_manager.install_type
        last_update = self.dl_manager.last_update
        # Send menu
        menu_data = hidden_data({
            "user_id": str(user_id),
            "version": version,
            "install_type": install_type,
            "last_update": last_update.isoformat(),
        })
        version_text = self._gallery_dl_version_text(version, install_type, last_update)
        await event.reply(
            f"{menu_data}{version_text}\nWould you like to update it now?",
            parse_mode="html",
            buttons=[[
                Button.inline("Update (stable)", "update:stable"),
                Button.inline("Update (dev)", "update:dev"),
                Button.inline("No thanks", "update:no"),
            ]],
        )
        raise events.StopPropagation

    async def handle_update_callback(self, event: events.CallbackQuery.Event) -> None:
        # Parse callback data
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"update:")
        # Parse menu data
        menu_msg = await event.get_message()
        menu_data = parse_hidden_data(menu_msg)
        user_id = int(menu_data["user_id"])
        version = menu_data["version"]
        install_type = menu_data["install_type"]
        last_update = datetime.datetime.fromisoformat(menu_data["last_update"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Check callback data
        if query_resp == b"stable":
            update_func = self.dl_manager.update_tool
        elif query_resp == b"dev":
            update_func = self.dl_manager.update_tool_prerelease
        elif query_resp == b"no":
            update_func = None
        else:
            await event.answer("Unrecognised update callback")
            raise events.StopPropagation
        # Deactivate menu
        await menu_msg.edit(
            self._gallery_dl_version_text(version, install_type, last_update),
            parse_mode="html",
            link_preview=False,
            buttons=None,
        )
        # Stop here if they said no
        if update_func is None:
            raise events.StopPropagation
        # Update gallery-dl
        evt = await event.reply("⏳ Updating gallery-dl", parse_mode="html")
        try:
            await update_func()
        except Exception as e:
            logger.warning("Failed to update gallery-dl", exc_info=e)
            await event.reply("Failed to update gallery-dl, error during update :(")
            await evt.delete()
            raise events.StopPropagation
        # Post completed message
        new_version = await self.dl_manager.get_tool_version()
        await event.reply(f"Updated gallery-dl to {new_version}")
        await evt.delete()
        raise events.StopPropagation

    async def raw_download(self, event: events.NewMessage.Event) -> None:
        raw_download_usage_count.inc()
        user_id = event.sender_id
        logger.info("Raw download request from %s", user_id)
        # Check if they are authorised to use the bot
        if not self.auth_manager.user_is_trusted(user_id):
            failed_auth_attempts.inc()
            logger.info("Unauthorised user has tried to run a raw download: %s", user_id)
            await event.reply("Apologies, you are not authorised to operate this bot")
            raise events.StopPropagation
        # Parse the message
        message_text = event.message.text
        message_split = shlex.split(message_text)
        dl_args = message_split[1:]
        if not dl_args:
            await event.reply("Please specify a link (and potentially arguments) for raw download")
            raise events.StopPropagation
        # Check if one of these args is a json dict
        dl_config = None
        for i, dl_arg in enumerate(dl_args[:]):
            try:
                dl_json = json.loads(dl_arg)
                if isinstance(dl_json, dict):
                    dl_config = dl_json
                    dl_args = dl_args[:i] + dl_args[i+1:]
            except json.decoder.JSONDecodeError:
                pass
        # If dl config was set, fetch base config and merge with it
        if dl_config is not None:
            config_path = await self.dl_manager.create_merged_config_file(dl_config)
            dl_args += ["-c", config_path]
        # Run the download
        await self._handle_link(dl_args, event, allow_auto_embed=False)
        raise events.StopPropagation

    # noinspection PyMethodMayBeStatic
    async def unknown_command(self, event: events.NewMessage.Event) -> None:
        # Only trigger in private message
        if event.chat_id == event.sender_id:
            unknown_command_usage_count.inc()
            await event.reply("Sorry, I do not understand that command")
            raise events.StopPropagation
