import html
import logging
import re

from telethon import TelegramClient, events

from gallery_dl_sub_bot.gallery_dl_manager import GalleryDLManager
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
        for link in fixed_links:
            resp = await self.dl_manager.run(["--dump-json", link])
            await event.respond(f"Gallery DL said:\n<pre>{html.escape(resp)}</pre>", parse_mode="html")
        raise events.StopPropagation
