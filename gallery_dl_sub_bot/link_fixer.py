import json
import logging
import urllib.parse
from abc import ABC
from typing import Optional

import yaml
from jinja2 import Environment, BaseLoader

logger = logging.getLogger(__name__)


def link_to_str(link: str | list[str]) -> str:
    return link if isinstance(link, str) else " ".join(link)


class LinkMatcher(ABC):
    """
    A LinkMatcher is a  pattern which checks whether a link matches a certain configuration.
    Often, that is the domain (Or "netloc") of a link. But other attributes of parsed URLs can also be matched.
    """
    def __init__(self, link_match: str | dict[str, str]) -> None:
        self.link_match = link_match

    def matches_link(self, link: str) -> bool:
        parsed = urllib.parse.urlparse(link)
        if isinstance(self.link_match, str):
            return parsed.netloc == self.link_match
        for key, val in self.link_match.items():
            if getattr(parsed, key) != val:
                return False
        return True


class LinkFix(LinkMatcher):
    """
    A LinkFix entry defines how to clean a link, using configuration from a dictionary
    """
    def __init__(self, link_match: str | dict[str, str], link_target: str | dict[str, str]) -> None:
        super().__init__(link_match)
        self.link_target = link_target

    def fix_link(self, link: str) -> str:
        parsed = urllib.parse.urlparse(link)
        if isinstance(self.link_target, str):
            parsed = parsed._replace(**{"netloc": self.link_target})
        else:
            parsed = parsed._replace(**self.link_target)
        # noinspection PyTypeChecker
        # (For some reason, it thinks this returns Literal[b""])
        return urllib.parse.urlunparse(parsed)


class CaptionOverride(LinkMatcher):
    """
    A CaptionOverride entry will, if it matches a given subscription link, parse the gallery-dl JSON entry for the item
    and construct a replacement caption using that data.
    Often this is to facilitate having a link to the specific post, rather than a caption describing the subscription.
    """
    def __init__(self, link_match: str | dict[str, str], caption_template: str) -> None:
        super().__init__(link_match)
        self.caption_template = caption_template

    def render_caption(self, data: dict) -> str:
        template = Environment(loader=BaseLoader()).from_string(self.caption_template)
        # TODO: dict to object or something
        return template.render(data=data)


class LinkFixer:
    """
    The LinkFixer contains all the configuration for all subscription link cleaning, and post caption overrides.
    It loads them from the yaml and parses them, and manages finding matching link cleaners and caption overriders for
    given links.
    """
    def __init__(self):
        self.fixes: list[LinkFix] = []
        self.caption_overrides: list[CaptionOverride] = []
        self.load_fixes()

    def load_fixes(self) -> None:
        with open("link_fixes.yaml", "r") as f:
            try:
                fix_data = yaml.safe_load(f)
            except yaml.YAMLError as e:
                logger.error("Failed to load link fixes settings from file", exc_info=e)
        # Load link fixes from config
        new_fixes: list[LinkFix] = []
        for fix in fix_data["link_fixes"]:
            if "from" not in fix:
                logger.error(f"Link fix in settings missing 'from' field: {fix}")
                raise ValueError("Link fix in settings missing 'from' field")
            if "to" not in fix:
                logger.error(f"Link fix in settings missing 'to' field: {fix}")
                raise ValueError("Link fix in settings missing 'to' field")
            new_fixes.append(LinkFix(fix["from"], fix["to"]))
        # Load caption overrides from config
        new_caption_overrides: list[CaptionOverride] = []
        for caption_override in fix_data.get("caption_overrides", []):
            if "match" not in caption_override:
                logger.error(f"Caption override in settings missing 'match' field: {caption_override}")
                raise ValueError("Caption override in settings missing 'match' field")
            if "caption" not in caption_override:
                logger.error("Caption override in settings missing 'caption' field: {caption_override}")
                raise ValueError("Caption override in settings missing 'caption' field")
            new_caption_overrides.append(CaptionOverride(caption_override["match"], caption_override["caption"]))
        self.fixes = new_fixes
        self.caption_overrides = new_caption_overrides

    def fix_link(self, link: str) -> str:
        for fix in self.fixes:
            if fix.matches_link(link):
                link = fix.fix_link(link)
        return link

    def override_caption(self, link: str | list[str], data_filename: str) -> Optional[str]:
        if not isinstance(link, str):
            return None
        for override in self.caption_overrides:
            if override.matches_link(link):
                try:
                    with open(data_filename, "r") as f:
                        data = json.load(f)
                except Exception as e:
                    logger.warning("Failed to open post metadata to format caption", exc_info=e)
                    return None
                return override.render_caption(data)
        return None

    # noinspection PyMethodMayBeStatic
    def link_to_filename(self, link: str) -> str:
        """
        Convert a given link into a zip filename, for more clarity of download
        """
        # Clean schema
        link = link.removeprefix("http://").removeprefix("https://")
        # Remove unnecessary prefix
        link = link.removeprefix("www.")
        # Remove extra slashes from the end
        while link.endswith("/"):
            link = link.removesuffix("/")
        # Replace common web TLDs
        link = link.replace(".com/", "_").replace(".co.uk/", "_").replace(".net", "_")
        # Replace characters with underscores
        link = link.replace(".", "_").replace("/", "_")
        # Ensure no double underscores
        while "__" in link:
            link = link.replace("__", "_")
        # Return that
        return link
