import asyncio
import html
import logging
import pathlib
import re
import shutil
import uuid

from telethon import TelegramClient, events, Button

from gallery_dl_sub_bot.auth_manager import AuthManager
from gallery_dl_sub_bot.gallery_dl_manager import GalleryDLManager
from gallery_dl_sub_bot.hidden_data import parse_hidden_data, hidden_data
from gallery_dl_sub_bot.link_fixer import LinkFixer

logger = logging.getLogger(__name__)


class Bot:

    def __init__(self, config: dict) -> None:
        self.config = config
        self.client = TelegramClient(
            "gallery_dl_sub_bot", self.config["telegram"]["api_id"], self.config["telegram"]["api_hash"]
        )
        self.dl_manager = GalleryDLManager("config_gallery_dl.json")
        self.auth_manager = AuthManager("trusted_users.yaml")
        self.sub_manager = SubscriptionManager()
        self.link_fixer = LinkFixer()

    def run(self) -> None:
        self.client.start(bot_token=self.config["telegram"]["bot_token"])
        # Register functions
        self.client.add_event_handler(self.start, events.NewMessage(pattern="/start", incoming=True))
        self.client.add_event_handler(self.boop, events.NewMessage(pattern="/beep", incoming=True))
        self.client.add_event_handler(self.check_for_links, events.NewMessage(incoming=True))
        self.client.add_event_handler(self.handle_zip_callback, events.CallbackQuery(pattern="dl_zip:"))
        self.client.add_event_handler(self.handle_subscribe_callback, events.CallbackQuery(pattern="subscribe:"))
        # Start listening
        try:
            logger.info("Starting bot")
            self.client.run_until_disconnected()
        finally:
            logger.info("Bot sleepy bye-bye time")

    async def boop(self, event: events.NewMessage.Event) -> None:
        await event.reply("Boop!")
        raise events.StopPropagation

    async def start(self, event: events.NewMessage.Event) -> None:
        await event.reply("Hey there! I'm not a very good bot yet, I'm quite early in development.")
        raise events.StopPropagation

    async def check_for_links(self, event: events.NewMessage.Event) -> None:
        if not self.auth_manager.user_is_trusted(event.message.peer_id.user_id):
            await event.reply("Apologies, you are not authorised to operate this bot")
            raise events.StopPropagation
        link_regex = re.compile(r"(https?://|www\.|\S+\.com)\S+", re.I)
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
        fixed_links = [self.link_fixer.fix_link(l) for l in links]  # TODO remove duplicates
        if len(fixed_links) > 1:
            # Tell the user all the links
            lines = [f"- {html.escape(l)}" for l in fixed_links]
            await event.reply("Found these links:\n" + "\n".join(lines), parse_mode="html")
        # Check them in gallery-dl
        await asyncio.gather(*(self._handle_link(link, event) for link in fixed_links))
        raise events.StopPropagation

    async def _handle_link(self, link: str, event: events.NewMessage.Event) -> None:
        dl_path = f"store/downloads/{uuid.uuid4()}/"
        evt = await event.reply(f"⏳ Downloading link: {html.escape(link)}", parse_mode="html")
        try:
            # TODO: queueing
            # TODO: in progress message
            resp = await self.dl_manager.run(["--write-metadata", "--write-info-json", "-d", dl_path, link])
        except Exception as e:
            logger.error(f"Failed to download link {link}", exc_info=e)
            await evt.reply(f"Failed to download link {html.escape(link)} :(")
            raise e
        lines = resp.strip().split("\n")
        await event.reply(f"Found {len(lines)} images(s) in link: {html.escape(link)}", parse_mode="html")
        await evt.delete()
        if len(lines) < 10:
            await event.reply(f"{html.escape(link)}", parse_mode="html", file=lines)
        else:
            hidden_link = hidden_data({
                "path": dl_path,
                "link": link,
            })
            await event.reply(
                f"Would you like to download these files as a zip?{hidden_link}",
                parse_mode="html",
                buttons=[[
                    Button.inline("Yes", "dl_zip:yes"),
                    Button.inline("No thanks", "dl_zip:no"),
                ]]
            )
            await event.reply(
                f"Would you like to subscribe to {html.escape(link)}?{hidden_link}",
                parse_mode="html",
                buttons=[[
                    Button.inline("Yes, subscribe", "subscribe:yes"),
                    Button.inline("No thanks", "subscribe:no"),
                ]]
            )

    async def handle_zip_callback(self, event: events.CallbackQuery.Event) -> None:
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"dl_zip:")
        logger.info(f"Callback query pressed: {query_data}")
        menu_msg = await event.get_message()
        menu_data = parse_hidden_data(menu_msg)
        dl_path = menu_data["path"]
        link = menu_data["link"]
        if query_resp == b"no":
            await menu_msg.delete()
            logger.info(f"Removing download path: {dl_path}")
            shutil.rmtree(dl_path)
            raise events.StopPropagation
        if query_resp == b"yes":
            await menu_msg.edit("⏳ Creating zip archive...", buttons=None)
            zip_path = f"store/downloads/{uuid.uuid4()}"
            shutil.make_archive(zip_path, "zip", dl_path)
            link_msg = await menu_msg.get_reply_message()
            await link_msg.reply(f"Here is the zip archive of {html.escape(link)}", file=f"{zip_path}.zip")
            await menu_msg.delete()
            raise events.StopPropagation
        await event.answer("Unrecognised response")

    async def handle_subscribe_callback(self, event: events.CallbackQuery.Event) -> None:
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"subscribe:")
        logger.info(f"Callback query pressed: {query_data}")
        menu_msg = await event.get_message()
        menu_data = parse_hidden_data(menu_msg)
        dl_path = menu_data["path"]
        link = menu_data["link"]
        if query_resp == b"no":
            await menu_msg.delete()
            raise events.StopPropagation
        if query_resp == b"yes":
            menu_msg.edit("⏳ Subscribing...", buttons=None)
            sub_id = uuid.uuid4()
            sub_path = f"store/subscriptions/{sub_id}"
            if pathlib.Path(dl_path).exists():
                shutil.copy2(dl_path, sub_path)
            menu_msg.reply("Actually, I don't know how to handle this yet.")  # TODO
        await event.answer("Unrecognised response")
