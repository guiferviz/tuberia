import re
import textwrap
from typing import Dict, List

import pydantic
import pytest

from tuberia.table import Table, make_flow
from tuberia.visualization import (
    flow_to_mermaid_code,
    open_mermaid_flow_in_browser,
)


@pytest.fixture(scope="module")
def flow():
    class TableOne(Table):
        database: str = "my_database"
        name: str = "one"

    class TableTwo(Table):
        database: str = "my_database"
        source_table: Table
        letter: str
        name: str = "two_{letter}"

        @pydantic.root_validator
        def format_name(cls, values) -> dict:
            if "name" in values:
                values["name"] = values["name"].format(**values)
            return values

    class TableConcat(Table):
        database: str = "my_database"
        name: str = "concat"
        tables_to_concat: List[Table]

    table_one = TableOne()
    table_two_a = TableTwo(source_table=table_one, letter="a")
    table_two_b = TableTwo(source_table=table_one, letter="b")
    table_concat = TableConcat(tables_to_concat=[table_two_a, table_two_b])
    flow = make_flow([table_concat])
    open_mermaid_flow_in_browser(flow)
    return flow


def test_flow_to_mermaid_code(flow):
    """Check if flow_to_mermaid_code generates the right mermaid code.

    The Flow object is not deterministic: it can generate different mermaid
    IDs per execution. As we know where the node declarations are (lines 1 to
    5 both included) we can extract the id and check that the edges are created
    accordingly.

    Example of expected code:

        graph TD
            task0["my_database.one"]
            task1["my_database.concat"]
            task2["my_database.two_a"]
            task4["my_database.two_b"]
            task0 --> task2
            task0 --> task4
            task2 --> task3
            task3 --> task1
            task4 --> task3
    """

    actual = flow_to_mermaid_code(flow).split("\n")
    assert actual[0] == "graph TD"
    mapping = get_task_name_id_mapping_from_mermaid(actual[1:5])
    assert set(actual[5:]) == set(
        [
            f"    {mapping['my_database.one']} --> {mapping['my_database.two_a']}",
            f"    {mapping['my_database.one']} --> {mapping['my_database.two_b']}",
            f"    {mapping['my_database.two_a']} --> {mapping['my_database.concat']}",
            f"    {mapping['my_database.two_b']} --> {mapping['my_database.concat']}",
        ]
    )


def test_open_mermaid_flow_in_browser(flow, mocker):
    mocker.patch(
        "tuberia.visualization.flow_to_mermaid_code",
        return_value=textwrap.dedent(
            """
                graph TD
                    task0["one"]
                    task1["concat"]
                    task2["two(letter=a)"]
                    task3["List"]
                    task4["two(letter=b)"]
                    task0 --> task2
                    task0 --> task4
                    task2 --> task3
                    task3 --> task1
                    task4 --> task3
            """
        ).lstrip("\n"),
    )
    webbrowser_open = mocker.patch("webbrowser.open")
    open_mermaid_flow_in_browser(flow)
    expected_url = "https://mermaid.ink/svg/Z3JhcGggVEQKICAgIHRhc2swWyJvbmUiXQogICAgdGFzazFbImNvbmNhdCJdCiAgICB0YXNrMlsidHdvKGxldHRlcj1hKSJdCiAgICB0YXNrM1siTGlzdCJdCiAgICB0YXNrNFsidHdvKGxldHRlcj1iKSJdCiAgICB0YXNrMCAtLT4gdGFzazIKICAgIHRhc2swIC0tPiB0YXNrNAogICAgdGFzazIgLS0+IHRhc2szCiAgICB0YXNrMyAtLT4gdGFzazEKICAgIHRhc2s0IC0tPiB0YXNrMwo="
    webbrowser_open.assert_called_with(expected_url, new=2)


def get_task_name_id_mapping_from_mermaid(code: List[str]) -> Dict[str, str]:
    regex = re.compile(r'^    (\w+)\["(.+)"\]$')
    task_name_id_mapping = {}
    for i in code:
        match = regex.match(i)
        assert match, f"{i} not matching regex"
        task_id, task_name = match.groups()
        task_name_id_mapping[task_name] = task_id
    return task_name_id_mapping
