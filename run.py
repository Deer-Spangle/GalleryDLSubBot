import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import click

from gallery_dl_sub_bot.bot import Bot
from gallery_dl_sub_bot.config import BotConfig


def setup_logging(log_level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)

    # Gallery DL sub bot log, for diagnosing the bot. Should not contain user information.
    bot_logger = logging.getLogger("gallery_dl_sub_bot")
    file_handler = TimedRotatingFileHandler("logs/gallery_dl_sub_bot.log", when="midnight")
    file_handler.setFormatter(formatter)
    bot_logger.addHandler(file_handler)
    bot_logger.setLevel(log_level.upper())


@click.command()
@click.option("--log-level", type=str, help="Log level for the logger", default="INFO")
@click.option("--subscriptions/--no-subscriptions", default=True)
def main(
        log_level: str,
        subscriptions: bool,
):
    setup_logging(log_level)
    config = BotConfig.load_config("config.json")
    config.enable_subscriptions = subscriptions
    bot = Bot(config)
    bot.run()


if __name__ == '__main__':
    main()