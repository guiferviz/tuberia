from tuberia.base_settings import BaseSettings


class TuberiaDatabricksSettings(BaseSettings, env_prefix="tuberia_databricks_"):
    default_table_name_underscore: bool = True
