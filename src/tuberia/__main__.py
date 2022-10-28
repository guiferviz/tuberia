import typer

from tuberia import greet
from tuberia.spark import get_spark
from tuberia.utils import get_flow_from_qualified_name

app = typer.Typer()


@app.command()
def visualize(flow_name: str):
    """Create a mermaid.js flowchart of your flow."""

    print("Visualizing flow", flow_name)
    flow_class = get_flow_from_qualified_name(flow_name)
    flow = flow_class()  # type: ignore
    flow.visualize()


@app.command()
def deploy(flow: str, format: str):
    """Deploy a flow."""

    print("deploying flow", flow)
    raise NotImplementedError()


@app.command()
def run(flow_name: str):
    """Run a flow."""

    print("Running flow", flow_name)
    flow_class = get_flow_from_qualified_name(flow_name)
    flow = flow_class()  # type: ignore
    flow.run()


@app.command()
def debug():
    """Create a spark session just for debugging."""

    spark = get_spark()
    print(f"{spark=}")
    breakpoint()


def version_callback(version: bool):
    if version:
        greet()
        raise typer.Exit()


@app.callback()
def callback(
    _: bool = typer.Option(False, "-v", "--version", callback=version_callback)
):
    """Tuberia CLI."""


def main():
    app()


if __name__ == "__main__":
    main()
