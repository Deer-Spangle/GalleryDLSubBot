from __future__ import annotations
import dataclasses
import json
from typing import Optional


@dataclasses.dataclass
class BotTelegramConfig:
    api_id: int
    api_hash: str
    bot_token: str
    session_suffix: Optional[str] = None

    @classmethod
    def from_json(cls, data: dict) -> BotTelegramConfig:
        return cls(
            api_id=data["api_id"],
            api_hash=data["api_hash"],
            bot_token=data["bot_token"],
            session_suffix=data.get("session_suffix"),
        )


@dataclasses.dataclass
class BotConfig:
    telegram: BotTelegramConfig
    enable_subscriptions: bool  # If false, don't run the subscriptions process

    @classmethod
    def from_json(cls, data: dict) -> BotConfig:
        return cls(
            telegram=BotTelegramConfig.from_json(data["telegram"]),
            enable_subscriptions=data.get("enable_subscriptions", True),
        )

    @classmethod
    def load_config(cls, filename: str) -> BotConfig:
        with open(filename, "r") as f:
            data = json.load(f)
        return cls.from_json(data)
