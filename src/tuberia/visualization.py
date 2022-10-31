import base64
import webbrowser
from typing import List

MERMAID_URL = "https://mermaid.ink/svg/"
TAB = "    "


def flow_to_mermaid_code(
    flow,
    names_as_ids: bool = False,
    show_constant_values: bool = True,
    show_constant_names: bool = True,
    ignore_parameters: List[str] = [],
    links: bool = False,
) -> str:
    edges = flow.all_downstream_edges()
    tasks = list(flow.tasks)
    ids = [f"task{i}" for i in range(len(tasks))]
    if names_as_ids:
        ids = [
            i.name if i.name != "List" else id_ for i, id_ in zip(tasks, ids)
        ]
    constants = flow.constants
    graph = ["graph TD"]
    for i, t in enumerate(tasks):
        const_str = []
        for k, v in constants[t].items():
            if k in ignore_parameters:
                continue
            if show_constant_names and show_constant_values:
                const_str.append(f"{k}={v}")
            elif show_constant_names:
                const_str.append(f"{k}")
        const_str = ",".join(const_str)
        const_str = (
            f"({const_str})" if show_constant_names and const_str else ""
        )
        name = t.name + const_str
        if links:
            graph.append(
                f"""{TAB}{ids[i]}["<a href='../../tables/{t.table.__class__.__name__}'>{name}</a>"]"""
            )
        else:
            graph.append(f'{TAB}{ids[i]}["{name}"]')
    edges_str = []
    for i, t in enumerate(tasks):
        for j in edges[t]:
            j_index = tasks.index(j.downstream_task)
            edges_str.append(f"{TAB}{ids[i]} --> {ids[j_index]}")
    if names_as_ids:
        edges_str = list(set(edges_str))
    graph.append("\n".join(edges_str))
    for i, t in enumerate(tasks):
        if "yellow" in t.tags:
            graph.append(f"{TAB}style {ids[i]} fill:#ff0")
    return "\n".join(graph)


def open_mermaid_flow_in_browser(flow):
    graph = flow_to_mermaid_code(flow)
    graph_base64 = encode_to_base64(graph)
    url = MERMAID_URL + graph_base64
    open_url_in_new_tab(url)


def encode_to_base64(text: str) -> str:
    bytes = text.encode("ascii")
    base64_bytes = base64.b64encode(bytes)
    return base64_bytes.decode("ascii")


def open_url_in_new_tab(url: str) -> None:
    webbrowser.open(url, new=2)
