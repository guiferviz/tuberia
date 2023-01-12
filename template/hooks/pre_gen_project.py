import re

PYTHON_MODULE_REGEX = re.compile("^[a-z][a-z0-9_]+$")


def assert_python_module_name_follows_pep8():
    module_name = "{{ cookiecutter.module_name }}"
    assert PYTHON_MODULE_REGEX.match(module_name), (
        f"Python module name `{module_name}` does not follow PEP8."
        "Only lower case and underscores `_` (if really needed) are recommended."
    )


def main():
    assert_python_module_name_follows_pep8()


if __name__ == "__main__":
    main()
