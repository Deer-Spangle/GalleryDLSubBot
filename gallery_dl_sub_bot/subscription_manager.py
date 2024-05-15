import dataclasses
import datetime


@dataclasses.dataclass
class Subscription:
    link: str
    path: str
    chat_id: int
    creator_id: int
    created_date: datetime.datetime
    last_check_date: datetime.datetime


class SubscriptionManager:
    pass

