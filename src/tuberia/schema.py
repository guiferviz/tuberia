from typing import Any, Optional

from varname import varname


class column(str):
    def __new__(
        cls, dtype: Any = str, alias: Optional[str] = None, key: bool = False
    ):
        if alias is None:
            alias = str(varname())
        return str.__new__(cls, alias)

    def __init__(
        self, dtype: Any = str, alias: Optional[str] = None, key: bool = False
    ):
        self.dtype = dtype
        self.key = key
