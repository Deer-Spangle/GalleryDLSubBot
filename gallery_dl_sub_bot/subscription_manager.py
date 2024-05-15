import dataclasses
import datetime
import json


@dataclasses.dataclass
class Subscription:
    link: str
    path: str
    chat_id: int
    creator_id: int
    created_date: datetime.datetime
    last_check_date: datetime.datetime
    failed_checks: int
    last_successful_check_date: datetime.datetime

    def to_json(self) -> dict:
        return {
            "link": self.link,
            "path": self.path,
            "chat_id": self.chat_id,
            "creator_id": self.creator_id,
            "created_date": self.created_date.isoformat(),
            "last_check_date": self.last_check_date.isoformat(),
            "failed_checks": self.failed_checks,
            "last_successful_check_date": self.last_successful_check_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict) -> "Subscription":
        return cls(
            data["link"],
            data["path"],
            data["chat_id"],
            data["creator_id"],
            datetime.datetime.fromisoformat(data["created_date"]),
            datetime.datetime.fromisoformat(data["last_check_date"]),
            data["failed_checks"],
            datetime.datetime.fromisoformat(data["last_successful_check_date"]),
        )


class SubscriptionManager:
    CONFIG_FILE = "subscriptions.json"

    def __init__(self) -> None:
        try:
            with open(self.CONFIG_FILE, "r") as f:
                config_data = json.load(f)
        except FileNotFoundError:
            config_data = {}
        self.subscriptions = [
            Subscription.from_json(sub_data) for sub_data in config_data.get("subscriptions", [])
        ]

    def save(self) -> None:
        config_data = {
            "subscriptions": [s.to_json() for s in self.subscriptions[:]]
        }
        with open(self.CONFIG_FILE, "w") as f:
            json.dump(config_data, f)
