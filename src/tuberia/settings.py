from typing import Optional

from tuberia.base_settings import BaseSettings


class FlowSettings(BaseSettings):
    flow_class: str
    flow_settings: dict
    deployer_class: Optional[str]
    deployer_settings: Optional[dict]
