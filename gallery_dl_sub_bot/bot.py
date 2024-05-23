import asyncio
import html
import logging
import os
import re
import uuid

import aioshutil
from telethon import TelegramClient, events, Button

from gallery_dl_sub_bot.auth_manager import AuthManager
from gallery_dl_sub_bot.date_format import format_last_check
from gallery_dl_sub_bot.gallery_dl_manager import GalleryDLManager
from gallery_dl_sub_bot.hidden_data import parse_hidden_data, hidden_data
from gallery_dl_sub_bot.link_fixer import LinkFixer
from gallery_dl_sub_bot.subscription import SubscriptionDestination
from gallery_dl_sub_bot.subscription_manager import SubscriptionManager

logger = logging.getLogger(__name__)


async def _check_sender(evt: events.CallbackQuery.Event, allowed_user_id: int) -> None:
    if evt.sender_id != allowed_user_id:
        await evt.answer("Unauthorized menu use")
        raise events.StopPropagation


class Bot:
    SUBS_PER_MENU_PAGE = 10

    def __init__(self, config: dict) -> None:
        self.config = config
        self.client = TelegramClient(
            "gallery_dl_sub_bot", self.config["telegram"]["api_id"], self.config["telegram"]["api_hash"]
        )
        self.dl_manager = GalleryDLManager("config_gallery_dl.json")
        self.auth_manager = AuthManager("trusted_users.yaml")
        self.sub_manager = SubscriptionManager(self.client, self.dl_manager)
        self.link_fixer = LinkFixer()

    def run(self) -> None:
        self.client.start(bot_token=self.config["telegram"]["bot_token"])
        # Register functions
        self.client.add_event_handler(self.start, events.NewMessage(pattern="/start", incoming=True))
        self.client.add_event_handler(self.boop, events.NewMessage(pattern="/beep", incoming=True))
        self.client.add_event_handler(
            self.summon_subscription_menu,
            events.NewMessage(pattern="/subscriptions", incoming=True),
        )
        self.client.add_event_handler(self.check_for_links, events.NewMessage(incoming=True))
        self.client.add_event_handler(self.handle_zip_callback, events.CallbackQuery(pattern="dl_zip:"))
        self.client.add_event_handler(self.handle_subscribe_callback, events.CallbackQuery(pattern="subscribe:"))
        self.client.add_event_handler(self.page_subscriptions_menu, events.CallbackQuery(pattern="subs_offset:"))
        self.client.add_event_handler(self.view_subscription_menu, events.CallbackQuery(pattern="subs_menu:"))
        self.client.add_event_handler(self.handle_unsubscribe_callback, events.CallbackQuery(pattern="unsubscribe:"))
        self.client.add_event_handler(self.handle_pause_callback, events.CallbackQuery(pattern="pause:"))
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
        await event.reply("Boop!")
        raise events.StopPropagation

    # noinspection PyMethodMayBeStatic
    async def start(self, event: events.NewMessage.Event) -> None:
        await event.reply("Hey there! I'm not a very good bot yet, I'm quite early in development.")
        raise events.StopPropagation

    async def check_for_links(self, event: events.NewMessage.Event) -> None:
        if not self.auth_manager.user_is_trusted(event.message.peer_id.user_id):
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
        # Tell the user all the links
        if len(fixed_links) > 1:
            lines = [f"- {html.escape(link)}" for link in fixed_links]
            await event.reply("Found these links:\n" + "\n".join(lines), parse_mode="html", link_preview=False)
        # Check them in gallery-dl
        await asyncio.gather(*(self._handle_link(link, event) for link in fixed_links))
        raise events.StopPropagation

    async def _handle_link(self, link: str, event: events.NewMessage.Event) -> None:
        dl_path = f"store/downloads/{uuid.uuid4()}/"
        # TODO: If subscription exists, use that
        evt = await event.reply(f"⏳ Downloading link: {html.escape(link)}", parse_mode="html", link_preview=False)
        try:
            # TODO: queueing
            # TODO: in progress message
            lines = await self.dl_manager.download(link, dl_path)
        except Exception as e:
            logger.error(f"Failed to download link {link}", exc_info=e)
            await evt.reply(f"Failed to download link {html.escape(link)} :(")
            raise e
        await event.reply(f"Found {len(lines)} images(s) in link: {html.escape(link)}", parse_mode="html")
        await evt.delete()
        if len(lines) < 10:
            await event.reply(f"{html.escape(link)}", parse_mode="html", file=lines)
            await aioshutil.rmtree(dl_path)
        else:
            hidden_link = hidden_data({
                "path": dl_path,
                "link": link,
                "user_id": str(event.message.peer_id.user_id),
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
                ]],
                link_preview=False,
            )

    async def handle_zip_callback(self, event: events.CallbackQuery.Event) -> None:
        query_data = event.query.data
        query_resp = query_data.removeprefix(b"dl_zip:")
        logger.info(f"Callback query pressed: {query_data}")
        menu_msg = await event.get_message()
        # Parse menu data
        menu_data = parse_hidden_data(menu_msg)
        dl_path = menu_data["path"]
        link = menu_data["link"]
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Handle no button
        if query_resp == b"no":
            await menu_msg.delete()
            logger.info(f"Removing download path: {dl_path}")
            await aioshutil.rmtree(dl_path)
            raise events.StopPropagation
        # Handle yes button
        if query_resp == b"yes":
            await menu_msg.edit("⏳ Creating zip archive...", buttons=None)
            zip_filename = self.link_fixer.link_to_filename(link)
            zip_path = f"store/downloads/{zip_filename}"
            await aioshutil.make_archive(zip_path, "zip", dl_path)
            link_msg = await menu_msg.get_reply_message()
            await link_msg.reply(
                f"Here is the zip archive of {html.escape(link)}",
                file=f"{zip_path}.zip",
                parse_mode="html",
                link_preview=False,
            )
            await menu_msg.delete()
            await aioshutil.rmtree(dl_path)
            os.unlink(f"{zip_path}.zip")
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
        dl_path = menu_data["path"]
        link = menu_data["link"]
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Handle no button
        if query_resp == b"no":
            await menu_msg.delete()
            raise events.StopPropagation
        # Handle yes button press
        if query_resp == b"yes":
            await menu_msg.edit("⏳ Subscribing...", buttons=None)
            try:
                await self.sub_manager.create_subscription(link, menu_msg.chat.id, user_id, dl_path)
            except Exception as e:
                logger.error(f"Failed to subscribe to {link}", exc_info=e)
                await menu_msg.edit(
                    f"Failed to create subscription to {html.escape(link)}",
                    parse_mode="html",
                    link_preview=False,
                )
                raise e
            link_msg = await menu_msg.get_reply_message()
            await link_msg.reply(f"Subscription created for {html.escape(link)}", parse_mode="html", link_preview=False)
            await menu_msg.delete()
            raise events.StopPropagation
        # Handle other callback data
        await event.answer("Unrecognised response")

    async def summon_subscription_menu(self, event: events.NewMessage.Event) -> None:
        chat_id = event.chat.id
        user_id = event.message.peer_id.user_id
        sub_dests = self.sub_manager.list_subscriptions(chat_id, user_id)
        if len(sub_dests) == 0:
            await event.reply("You have no subscriptions in this chat. Send a link to create one")
            raise events.StopPropagation
        await event.reply(
            self._list_subscriptions_menu_text(sub_dests, 0, user_id),
            parse_mode="html",
            link_preview=False,
            buttons=self._list_subscriptions_menu_buttons(sub_dests, 0),
        )
        raise events.StopPropagation

    def _list_subscriptions_menu_buttons(self, subs: list[SubscriptionDestination], offset: int) -> list[list[Button]]:
        # Cap offset
        if offset < 0:
            offset = 0
        if offset >= len(subs):
            offset = len(subs) - 1
        # Get the page's subscription list
        subs_page = subs[offset:offset+self.SUBS_PER_MENU_PAGE]
        # Construct the pagination buttons
        has_prev = offset > 0
        has_next = len(subs) > self.SUBS_PER_MENU_PAGE + offset
        pagination_row = []
        if has_prev:
            prev_offset = max(offset-self.SUBS_PER_MENU_PAGE, 0)
            pagination_row.append(Button.inline("⬅️Prev", f"subs_offset:{prev_offset}"))
        if has_next:
            next_offset = offset + self.SUBS_PER_MENU_PAGE
            pagination_row.append(Button.inline("➡️Next", f"subs_offset:{next_offset}"))
        # Construct button list
        return [
                [Button.inline(f"{n}) {sub.subscription.link}", f"subs_menu:{n}")]
                for n, sub in enumerate(subs_page, start=1+offset)
            ] + [
            pagination_row
        ]

    def _list_subscriptions_menu_text(self, subs: list[SubscriptionDestination], offset: int, user_id: int) -> str:
        menu_data = hidden_data({"offset": str(offset), "user_id": str(user_id)})
        menu_text = f"{menu_data}You have {len(subs)} subscriptions in this chat:\n"
        lines = []
        for n, sub in enumerate(subs, start=1):
            bpt = "-"
            idx = n - 1
            if offset <= idx < offset + self.SUBS_PER_MENU_PAGE:
                bpt = "*"
            suffix = ""
            if sub.subscription.failed_checks > 0:
                suffix = " (failing checks)"
            if sub.paused:
                suffix = " (paused)"
            lines.append(f"{bpt} {n}) {html.escape(sub.subscription.link)}{suffix}")
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
        chat_id = event.chat.id
        sub_dests = self.sub_manager.list_subscriptions(chat_id, user_id)
        # Handle empty subscription list
        if len(sub_dests) == 0:
            await menu_msg.edit("You have no subscriptions in this chat. Send a link to create one")
            raise events.StopPropagation
        # Send menu
        await menu_msg.edit(
            self._list_subscriptions_menu_text(sub_dests, offset, user_id),
            parse_mode="html",
            link_preview=False,
            buttons=self._list_subscriptions_menu_buttons(sub_dests, offset),
        )
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
        chat_id = event.chat.id
        sub_dests = self.sub_manager.list_subscriptions(chat_id, user_id)
        # Handle empty subscription list
        if len(sub_dests) == 0:
            await menu_msg.edit("You have no subscriptions in this chat. Send a link to create one")
            raise events.StopPropagation
        # Check subscription index is valid
        if 0 > view_sub_idx or len(sub_dests) <= view_sub_idx:
            await event.answer("Subscription index not valid")
            await menu_msg.edit(
                self._list_subscriptions_menu_text(sub_dests, offset, user_id),
                parse_mode="html",
                link_preview=False,
                buttons=self._list_subscriptions_menu_buttons(sub_dests, offset),
            )
            raise events.StopPropagation
        # Get subscription and destination
        sub_dest = sub_dests[view_sub_idx]
        sub = sub_dest.subscription
        # Assemble menu data
        msg_data = {
            "path": sub.path,
            "link": sub.link,
            "user_id": user_id,
        }
        # Send menu
        view_sub_lines = [f"{hidden_data(msg_data)}Viewing subscription: {html.escape(sub.link)}"]
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
        await menu_msg.edit(
            f"⏳ Unsubscribing from {html.escape(link)}...",
            parse_mode="html",
            link_preview=False,
            buttons=None,
        )
        chat_id = event.chat.id
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
        link = menu_data["link"]
        user_id = int(menu_data["user_id"])
        # Check button is pressed by user who summoned the menu
        await _check_sender(event, user_id)
        # Check callback data
        if query_resp == b"pause":
            pause_sub = True
        elif query_resp == b"resume":
            pause_sub = False
        else:
            await event.answer("Unrecognised pause callback")
            raise events.StopPropagation
        # Pause subscription
        chat_id = event.chat.id
        await self.sub_manager.pause_subscription(link, chat_id, pause_sub)
        # Refresh menu menu
        pause_verb = "Paused" if pause_sub else "Resumed"
        await menu_msg.edit(
            f"{pause_verb} subscription to {html.escape(link)}",
            parse_mode="html",
            link_preview=False,
            buttons=None,
        )
