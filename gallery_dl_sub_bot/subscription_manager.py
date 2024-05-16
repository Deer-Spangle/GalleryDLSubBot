import dataclasses
import datetime
import json
import shutil
import uuid
from typing import Optional


@dataclasses.dataclass
class SubscriptionDestination:
    chat_id: int
    creator_id: int
    created_date: datetime.datetime

    def to_json(self) -> dict:
        return {
            "chat_id": self.chat_id,
            "creator_id": self.creator_id,
            "created_date": self.created_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict) -> "SubscriptionDestination":
        return cls(
            data["chat_id"],
            data["creator_id"],
            datetime.datetime.fromisoformat(data["created_date"]),
        )


@dataclasses.dataclass
class Subscription:
    link: str
    path: str
    destinations: list[SubscriptionDestination]
    last_check_date: datetime.datetime
    failed_checks: int
    last_successful_check_date: datetime.datetime

    def to_json(self) -> dict:
        return {
            "link": self.link,
            "path": self.path,
            "destinations": [d.to_json() for d in self.destinations],
            "last_check_date": self.last_check_date.isoformat(),
            "failed_checks": self.failed_checks,
            "last_successful_check_date": self.last_successful_check_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict) -> "Subscription":
        return cls(
            data["link"],
            data["path"],
            [SubscriptionDestination.from_json(d) for d in data["destinations"]],
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

    def sub_for_link(self, link: str) -> Optional[Subscription]:
        for sub in self.subscriptions[:]:
            if sub.link == link:
                return sub
        return None

    async def create_subscription(self, link: str, chat_id: int, creator_id: int, current_path: str) -> Subscription:
        # See if a subscription already exists for this link
        matching_sub = self.sub_for_link(link)
        # Figure out new path
        new_path = f"store/subscriptions/{uuid.uuid4()}"
        if matching_sub:
            new_path = matching_sub.path
        # Copy files
        if not matching_sub:
            shutil.copy2(current_path, new_path)
        # Create destination
        now_date = datetime.datetime.now(datetime.timezone.utc)
        dest = SubscriptionDestination(
            chat_id,
            creator_id,
            now_date
        )
        # Extend or create subscription
        if matching_sub:
            matching_sub.destinations.append(dest)
            return matching_sub
        sub = Subscription(
            link,
            new_path,
            [dest],
            now_date,
            0,
            now_date,
        )
        self.subscriptions.append(sub)
        return sub

    async def remove_subscription(self, link: str, chat_id: int) -> None:
        matching_sub = self.sub_for_link(link)
        found_dest = None
        for dest in matching_sub.destinations:
            if dest.chat_id == chat_id:
                found_dest = dest
        if not found_dest:
            raise ValueError("Cannot find matching subscription for this link and chat")
        matching_sub.destinations.remove(found_dest)
        if len(matching_sub.destinations) == 0:
            self.subscriptions.remove(matching_sub)
            shutil.rmtree(matching_sub.path)
