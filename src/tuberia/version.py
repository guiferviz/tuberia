import importlib.metadata

try:
    __version__ = importlib.metadata.version("tuberia")
except ImportError:
    # When we run Python scripts from inside the package without installing it
    # an ImportError is raised. For example, running
    # `python src/tuberia/__main__.py` in an environment without the package
    # installed causes this error.
    __version__ = "0.0.0"
