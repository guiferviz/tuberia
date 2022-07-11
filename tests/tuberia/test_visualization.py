import re
import textwrap
from typing import Dict, List

import pytest
from prefect import Flow

from tuberia.table import Table, table
from tuberia.visualization import (
    flow_to_mermaid_code,
    open_mermaid_flow_in_browser,
)


@pytest.fixture(scope="module")
def flow():
    @table
    def one() -> Table:
        return Table(database="my_database", name="one")

    @table
    def two(one: Table, letter: str) -> Table:
        return Table(database="my_database", name="two")

    @table
    def concat(tables: List[Table]) -> Table:
        print(f"table concat created from {', '.join(i.name for i in tables)}")
        return Table(database="my_database", name="concat")

    with Flow("test") as flow:
        one_table = one()
        two_a_table = two(one_table, "a")
        two_b_table = two(one_table, "b")
        concat([two_a_table, two_b_table])
    return flow


def test_flow_to_mermaid_code(flow):
    """Check if flow_to_mermaid_code generates the right mermaid code.

    The Flow object is not deterministic: it can generate different mermaid
    IDs per execution. As we know where the node declarations are (lines 1 to
    5 both included) we can extract the id and check that the edges are created
    accordingly.

    Example of expected code:

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

    actual = flow_to_mermaid_code(flow).split("\n")
    assert actual[0] == "graph TD"
    mapping = get_task_name_id_mapping_from_mermaid(actual[1:6])
    assert set(actual[6:]) == set(
        [
            f"    {mapping['one']} --> {mapping['two(letter=a)']}",
            f"    {mapping['one']} --> {mapping['two(letter=b)']}",
            f"    {mapping['two(letter=a)']} --> {mapping['List']}",
            f"    {mapping['two(letter=b)']} --> {mapping['List']}",
            f"    {mapping['List']} --> {mapping['concat']}",
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
