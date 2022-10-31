import pathlib
import textwrap
from typing import List, Tuple

from tuberia.flow import Flow, make_prefect_flow
from tuberia.table import Table
from tuberia.visualization import flow_to_mermaid_code


def document_flow(flow: Flow, root_dir="docs"):
    documentation_dict = _create_documentation_dict()
    _populate_documentation_dict(flow, documentation_dict)
    _write_documentation_dict(
        documentation_dict, root_dir=root_dir, pprint=False
    )


def _create_documentation_dict():
    return {
        "flows": {},
        "tables": {},
    }


def _populate_documentation_dict(flow: Flow, documentation_dict: dict):
    documentation_dict["flows"][flow.__class__.__name__] = _flow_document(flow)
    for i in flow.list_tables():
        documentation_dict["tables"][i.__class__.__name__] = _table_document(i)


def _flow_document(flow: Flow) -> str:
    title = flow.__class__.__name__
    docs = flow.__doc__
    mermaid_code = flow_to_mermaid_code(
        make_prefect_flow(flow.define()), links=True
    )
    content = f"# {title}\n\n"
    if docs:
        content += f"{_dedent_docs(docs)}\n\n"
    parameters = f"## Parameters\n\n```\n{flow.json(indent=4)}\n```\n\n"
    tables_flow = f"## Tables flow\n\n```mermaid\n{mermaid_code}\n```\n\n"
    return f"{content}\n\n{parameters}{tables_flow}"


def _dedent_docs(docs: str) -> str:
    lines = docs.split("\n")
    if len(lines) > 1:
        first_line, other_lines = lines[0], lines[1:]
        docs = first_line + "\n" + textwrap.dedent("\n".join(other_lines))
    return docs


def _table_document(table: Table) -> str:
    title = table.__class__.__name__
    docs = table.__doc__
    content = f"# {title}\n\n"
    if docs:
        content += f"{_dedent_docs(docs)}\n\n"
    if table.schema_:
        schema_str = "## Schema\n\n| Column name | Type |\n|---|---|\n"
        for i in table.schema_:
            schema_str += f"|{i.name}|{i.dataType.typeName()}|\n"
        content += schema_str
    return content


def _write_documentation_dict(documentation_dict, root_dir="docs", pprint=True):
    if pprint:
        import pprint

        pprint.pprint(documentation_dict)
    else:
        pending_dicts: List[Tuple[str, dict]] = [(root_dir, documentation_dict)]
        while len(pending_dicts):
            directory, dictionary = pending_dicts.pop()
            for k, v in dictionary.items():
                k = f"{directory}/{k}"
                if isinstance(v, dict):
                    pending_dicts.append((k, v))
                else:
                    path = pathlib.Path(f"{k}.md")
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(v)
