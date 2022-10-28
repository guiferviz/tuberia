import json
import os
from typing import Any, Dict, Mapping, Tuple

from pydantic import BaseSettings as PydanticBaseSettings
from pydantic.env_settings import SettingsSourceCallable


def get_env_vars(case_sensitive: bool) -> Mapping[str, str]:
    """Return a dict with the environment variables of the system.

    If `case_sensitive` is False, all the keys will be in lower case.

    """
    if case_sensitive:
        return os.environ
    return {k.lower(): v for k, v in os.environ.items()}


def env_json_config_settings_source(
    settings: PydanticBaseSettings,
) -> Dict[str, Any]:
    config = settings.__config__
    env_vars = get_env_vars(config.case_sensitive)
    env_var_with_config = config.env_prefix + config.env_json_config_name  # type: ignore
    if not config.case_sensitive:
        env_var_with_config = env_var_with_config.lower()

    if env_var_with_config in env_vars:
        json_data = env_vars[env_var_with_config]
        try:
            return json.loads(json_data)
        except ValueError as error:
            raise ValueError(
                f"Invalid JSON in `{env_var_with_config}` env var"
            ) from error
    return {}


class BaseSettings(PydanticBaseSettings):
    class Config:
        env_prefix = "tuberia_"
        env_json_config_name = "json_settings"

        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> Tuple[SettingsSourceCallable, ...]:
            return (
                init_settings,
                env_settings,
                env_json_config_settings_source,
                file_secret_settings,
            )
