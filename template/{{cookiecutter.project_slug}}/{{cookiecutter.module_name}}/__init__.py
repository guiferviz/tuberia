from {{cookiecutter.module_name}}.version import __version__

ASCII_LOGO = f"""
╔╦╗╦ ╦╔╗ ╔═╗╦═╗╦╔═╗
 ║ ║ ║╠╩╗║╣ ╠╦╝║╠═╣
 ╩ ╚═╝╚═╝╚═╝╩╚═╩╩ ╩
{{ cookiecutter.project_name }} Version: {__version__}
""".strip()


def greet():
    print(ASCII_LOGO)
