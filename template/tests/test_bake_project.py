import pytest
from cookiecutter.exceptions import FailedHookException
from pytest_cookies.plugin import Cookies

import tests.utils as utils


def test_bake_project_with_default_options_works(cookies: Cookies):
    result = cookies.bake()

    assert result.exception is None
    assert result.exit_code == 0
    assert result.project.isdir()
    assert result.project.listdir(), "Output dir should contain some files"


def test_bake_project_with_non_pep8_module_name(cookies: Cookies):
    result = cookies.bake(extra_context=dict(project_name=":)"))

    assert result.exception is not None
    assert type(result.exception) == FailedHookException
    assert result.exit_code != 0


@pytest.mark.slow
def test_bake_project_and_run_tests(cookies: Cookies):
    result = cookies.bake()
    project_path_str = str(result.project)

    with utils.use_cwd(project_path_str):
        with utils.use_poetry():
            utils.check_call("make install")
            utils.check_call("make test")
