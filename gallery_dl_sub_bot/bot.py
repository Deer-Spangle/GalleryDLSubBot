import html
import logging
import re

from telethon import TelegramClient, events

logger = logging.getLogger(__name__)


class Bot:

    def __init__(self, config: dict) -> None:
        self.config = config
        self.client = TelegramClient(
            "gallery_dl_sub_bot", self.config["telegram"]["api_id"], self.config["telegram"]["api_hash"]
        )

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
        for link in link_regex.finditer(event.message.text):
            links.append(link.group(0))
        if event.message.buttons:
            for button_row in event.message.buttons:
                for button in button_row:
                    if button.url:
                        links.append(button.url)
        if not links:
            await event.respond("Could not find any links in that message")
            raise events.StopPropagation
        await event.respond("Found these links:\n" + "\n".join(html.escape(l) for l in links), parse_mode="html")
        raise events.StopPropagation
