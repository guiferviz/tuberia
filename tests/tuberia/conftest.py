import pytest
from loguru import logger


@pytest.fixture
def caplog(caplog):
    """Caplog fixture that works with loguru.

    Taken from:
    https://github.com/Delgan/loguru/issues/59#issuecomment-1016516449

    """
    handler_id = logger.add(caplog.handler, format="{message}")
    yield caplog
    logger.remove(handler_id)
