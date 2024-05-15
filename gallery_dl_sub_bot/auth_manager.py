import yaml


class AuthManager:
    def __init__(self, config_path: str) -> None:
        self.config_path = config_path

    def user_is_trusted(self, user_id: int) -> bool:
        with open(self.config_path, "r") as f:
            config_data = yaml.safe_load(f)
        trusted_users = config_data.get("trusted_users", [])
        return user_id in trusted_users
