import asyncio
import html
import logging
import re
import shutil
import uuid

from telethon import TelegramClient, events, Button

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
        self.link_fixer = LinkFixer()

    def run(self) -> None:
        self.client.start(bot_token=self.config["telegram"]["bot_token"])
        # Register functions
        self.client.add_event_handler(self.start, events.NewMessage(pattern="/start", incoming=True))
        self.client.add_event_handler(self.boop, events.NewMessage(pattern="/beep", incoming=True))
        self.client.add_event_handler(self.check_for_links, events.NewMessage(incoming=True))
        self.client.add_event_handler(self.handle_zip_callback, events.CallbackQuery(pattern="dl_zip:"))
        # Start listening
        try:
            logger.info("Starting bot")
            self.client.run_until_disconnected()
        finally:
            logger.info("Bot sleepy bye-bye time")

    async def boop(self, event: events.NewMessage.Event) -> None:
        await event.respond("Boop!")
        raise events.StopPropagation

    async def start(self, event: events.NewMessage.Event) -> None:
        await event.respond("Hey there! I'm not a very good bot yet, I'm quite early in development.")
        raise events.StopPropagation

    async def check_for_links(self, event: events.NewMessage.Event) -> None:
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
            await event.respond("Could not find any links in that message")
            raise events.StopPropagation
        # Fix all the links
        fixed_links = [self.link_fixer.fix_link(l) for l in links]
        # Tell the user the links
        await event.respond("Found these links:\n" + "\n".join(html.escape(l) for l in fixed_links), parse_mode="html")
        # Check them in gallery-dl
        await asyncio.gather(*(self._handle_link(link, event) for link in fixed_links))
        raise events.StopPropagation

    async def _handle_link(self, link: str, event: events.NewMessage.Event) -> None:
        dl_path = f"store/downloads/{uuid.uuid4()}/"
        evt = await event.respond(f"⏳ Downloading link: {html.escape(link)}", parse_mode="html")
        try:
            # TODO: queueing
            # TODO: in progress message
            resp = await self.dl_manager.run(["--write-metadata", "--write-info-json", "-d", dl_path, link])
        except Exception as e:
            logger.error(f"Failed to download link {link}", exc_info=e)
            await evt.respond(f"Failed to download link {html.escape(link)} :(")
            raise e
        lines = resp.strip().split("\n")
        await evt.respond(f"Downloaded {len(lines)} images(s)", parse_mode="html")
        await evt.delete()
        if len(lines) < 10:
            await evt.respond(f"{html.escape(link)}", parse_mode="html", file=lines)
        else:
            await evt.respond(
                f"Would you like to download these files as a zip?{hidden_data({'path': dl_path})}",
                parse_mode="html",
                buttons=[
                    [Button.inline("Yes", "dl_zip:yes")],
                    [Button.inline("No thanks", "dl_zip:no")],
                ]
            )
            await evt.respond(f"Would you like to subscribe to {html.escape(link)}?", parse_mode="html")  # TODO

    async def handle_zip_callback(self, event: events.CallbackQuery.Event) -> None:
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"dl_zip:")
        logger.info(f"Callback query pressed: {query_data}")
        menu_msg = await event.get_message()
        menu_data = parse_hidden_data(menu_msg)
        dl_path = menu_data["path"]
        if query_resp == b"no":
            await menu_msg.delete()
            logger.info(f"Removing download path: {dl_path}")
            shutil.rmtree(dl_path)
            raise events.StopPropagation
        if query_resp == b"yes":
            await menu_msg.edit("⏳ Creating zip archive...", buttons=None)
            zip_path = f"store/downloads/{uuid.uuid4()}"
            shutil.make_archive(zip_path, "zip", dl_path)
            await menu_msg.respond("Here is the zip archive of that feed", file=f"{zip_path}.zip")
            await menu_msg.delete()
            raise events.StopPropagation
