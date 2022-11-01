from typing import List

import pydantic

from tuberia import utils


def test_freeze():
    class Foo(pydantic.BaseModel):
        a: int = 3
        b: List[set] = [{4}]

    assert utils.freeze(Foo()) == frozenset(
        [("__class__", "Foo"), ("a", 3), ("b", tuple([tuple([4])]))]
    )
