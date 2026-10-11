"""Shared secret.cfg lookup for the bot and manage_streamer."""

import configparser
import os

DEFAULT_REGION = "us-east-1"
DEFAULT_STATE_TABLE = "discord-twitch-state"
DEFAULT_STREAMERS_TABLE = "discord-twitch-streamers"


def secret_candidates():
    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
    candidates = []
    if cred_dir:
        candidates.append(os.path.join(cred_dir, "secret.cfg"))
    candidates.extend(
        [
            "/etc/discord-twitch/secret.cfg",
            "/usr/local/discord-twitch/secret.cfg",
            "secret.cfg",
        ]
    )
    return candidates


def find_secret_path():
    for candidate in secret_candidates():
        if os.path.exists(candidate):
            return candidate
    return None


def load_parser():
    """Return (parser, path). Raises FileNotFoundError when nothing can be read."""
    path = find_secret_path()
    if not path:
        raise FileNotFoundError("❌ Failed to parse secret file: secret.cfg")

    parser = configparser.ConfigParser()
    parser.optionxform = str
    if not parser.read(path):
        raise FileNotFoundError(f"❌ Failed to parse secret file: {path}")
    return parser, path


def _server_setting(parser, key, default):
    if "server" not in parser:
        return default
    value = parser["server"].get(key, "").strip()
    return value or default


def dynamodb_settings(parser):
    """Region, state table, and streamers table from [server]."""
    region = _server_setting(parser, "dynamodb_region", DEFAULT_REGION)
    state_table = _server_setting(parser, "dynamodb_table", DEFAULT_STATE_TABLE)
    streamers_table = _server_setting(
        parser, "dynamodb_streamers_table", DEFAULT_STREAMERS_TABLE
    )
    return region, state_table, streamers_table
