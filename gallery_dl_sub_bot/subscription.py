import dataclasses
import datetime
from typing import Optional

@dataclasses.dataclass
class Download:
    link: str
    path: str
    last_check_date: datetime.datetime


@dataclasses.dataclass
class CompleteDownload(Download):

    def to_json(self) -> dict:
        return {
            "link": self.link,
            "path": self.path,
            "last_check_date": self.last_check_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict) -> "CompleteDownload":
        return cls(
            data["link"],
            data["path"],
            datetime.datetime.fromisoformat(data["last_check_date"]),
        )


@dataclasses.dataclass
class SubscriptionDestination:
    chat_id: int
    creator_id: int
    created_date: datetime.datetime
    paused: bool
    subscription: Optional["Subscription"] = None

    def to_json(self) -> dict:
        return {
            "chat_id": self.chat_id,
            "creator_id": self.creator_id,
            "created_date": self.created_date.isoformat(),
            "paused": self.paused,
        }

    @classmethod
    def from_json(cls, data: dict) -> "SubscriptionDestination":
        return cls(
            data["chat_id"],
            data["creator_id"],
            datetime.datetime.fromisoformat(data["created_date"]),
            data.get("paused", False),
        )


@dataclasses.dataclass
class Subscription(Download):
    destinations: list[SubscriptionDestination]
    failed_checks: int
    last_successful_check_date: datetime.datetime

    def __post_init__(self):
        for dest in self.destinations:
            dest.subscription = self

    def to_json(self) -> dict:
        return {
            "link": self.link,
            "path": self.path,
            "last_check_date": self.last_check_date.isoformat(),
            "destinations": [d.to_json() for d in self.destinations],
            "failed_checks": self.failed_checks,
            "last_successful_check_date": self.last_successful_check_date.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict) -> "Subscription":
        return cls(
            data["link"],
            data["path"],
            datetime.datetime.fromisoformat(data["last_check_date"]),
            [SubscriptionDestination.from_json(d) for d in data["destinations"]],
            data["failed_checks"],
            datetime.datetime.fromisoformat(data["last_successful_check_date"]),
        )

    def matching_chat(self, chat_id: int) -> Optional[SubscriptionDestination]:
        for dest in self.destinations:
            if dest.chat_id == chat_id:
                return dest
        return None

    def matching_dest(self, chat_id: int, user_id: int) -> Optional[SubscriptionDestination]:
        for dest in self.destinations:
            if dest.chat_id == chat_id and dest.creator_id == user_id:
                return dest
        return None