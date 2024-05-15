import logging
import urllib.parse

import yaml

logger = logging.getLogger(__name__)


class LinkFix:
    def __init__(self, link_match: str | dict[str, str], link_target: str | dict[str, str]) -> None:
        self.link_match = link_match
        self.link_target = link_target

    def matches_link(self, link: str) -> bool:
        parsed = urllib.parse.urlparse(link)
        if isinstance(self.link_match, str):
            return parsed.netloc == self.link_match
        for key, val in self.link_match.items():
            if getattr(parsed, key) != val:
                return False
        return True

    def fix_link(self, link: str) -> str:
        parsed = urllib.parse.urlparse(link)
        if isinstance(self.link_target, str):
            parsed = parsed._replace(**{"netloc": self.link_target})
        else:
            parsed = parsed._replace(**self.link_target)
        return urllib.parse.urlunparse(parsed)


class LinkFixer:
    def __init__(self):
        self.fixes: list[LinkFix] = []
        self.load_fixes()

    def load_fixes(self) -> None:
        with open("link_fixes.yaml", "r") as f:
            try:
                fix_data = yaml.safe_load(f)
            except yaml.YAMLError as e:
                logger.error("Failed to load link fixes settings from file", exc_info=e)
        new_fixes: list[LinkFix] = []
        for fix in fix_data["link_fixes"]:
            if "from" not in fix:
                logger.error(f"Link fix in settings missing 'from' field: {fix}")
                raise ValueError("Link fix in settings missing 'from' field")
            if "to" not in fix:
                logger.error(f"Link fix in settings missing 'to' field: {fix}")
                raise ValueError("Link fix in settings missing 'to' field")
            new_fixes.append(LinkFix(fix["from"], fix["to"]))
        self.fixes = new_fixes

    def fix_link(self, link: str) -> str:
        for fix in self.fixes:
            if fix.matches_link(link):
                link = fix.fix_link(link)
        return link
