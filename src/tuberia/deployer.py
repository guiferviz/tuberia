from __future__ import annotations

import abc

from tuberia import utils
from tuberia.base_settings import BaseSettings
from tuberia.flow import Flow


class Deployer(abc.ABC, BaseSettings):
    class Config:
        env_prefix = "tuberia_deployer_"
        env_json_config_name = "json_settings"

    @staticmethod
    def from_qualified_name(name: str) -> Deployer:
        module_name, class_name = name.rsplit(".", 1)
        return utils.get_module_member(module_name, class_name)

    @abc.abstractmethod
    def run(self, flow: Flow):
        raise NotImplementedError()
