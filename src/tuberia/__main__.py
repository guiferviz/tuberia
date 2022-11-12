import typer
from loguru import logger

from tuberia import docs, greet
from tuberia.deployer import Deployer
from tuberia.flow import Flow
from tuberia.spark import get_spark

app = typer.Typer()


@app.command()
def visualize(flow_name: str):
    """Create a mermaid.js flowchart of your flow."""

    print("Visualizing flow", flow_name)
    flow_class = Flow.from_qualified_name(flow_name)
    flow = flow_class()  # type: ignore
    flow.visualize()


@app.command()
def deploy(flow_name: str, deployer_name: str):
    """Deploy a flow."""

    logger.info(f"Deploying flow `{flow_name}` using `{deployer_name}`")
    flow_class = Flow.from_qualified_name(flow_name)
    flow = flow_class()  # type: ignore
    deployer_class = Deployer.from_qualified_name(deployer_name)
    deployer = deployer_class()  # type: ignore
    deployer.run(flow)


@app.command()
def run(flow_name: str):
    """Run a flow."""

    logger.info(f"Running flow `{flow_name}`")
    flow_class = Flow.from_qualified_name(flow_name)
    flow = flow_class()  # type: ignore
    flow.run()


@app.command()
def doc(flow_name: str):
    """Document a flow."""

    logger.info(f"Documenting flow `{flow_name}`")
    flow_class = Flow.from_qualified_name(flow_name)
    flow = flow_class()  # type: ignore
    docs.document_flow(flow)


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
    app(standalone_mode=False)


if __name__ == "__main__":
    main()
