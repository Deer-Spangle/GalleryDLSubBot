import json

from gallery_dl_sub_bot.bot import Bot

if __name__ == '__main__':
    with open("config.json", "r") as f:
        config = json.load(f)
    bot = Bot(config)
    bot.run()