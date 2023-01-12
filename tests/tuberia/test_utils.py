from typing import List

import pydantic

from tuberia import list_or_generator_or_value_to_list, utils


def test_freeze():
    class Foo(pydantic.BaseModel):
        a: int = 3
        b: List[set] = [{4}]

    assert utils.freeze(Foo()) == frozenset(
        [("__class__", "Foo"), ("a", 3), ("b", tuple([tuple([4])]))]
    )


class TestListOrGeneratorOrValueToList:
    def test_return_list(self):
        def return_list():
            return [3, 4]

        assert list_or_generator_or_value_to_list(return_list()) == [3, 4]

    def test_yield_values(self):
        def yield_values():
            yield 3
            yield 4

        assert list_or_generator_or_value_to_list(yield_values()) == [3, 4]

    def test_return_value(self):
        def return_value():
            return 3

        assert list_or_generator_or_value_to_list(return_value()) == [3]
