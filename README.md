<p align="center">
    <a href="https://aidictive.github.io/tuberia" target="_blank">
        <img src="docs/images/logo.png"
             alt="Tuberia logo"
             width="800">
    </a>
</p>
<p align="center">
    <a href="https://github.com/AIdictive/tuberia/actions/workflows/cicd.yaml" target="_blank">
        <img src="https://github.com/aidictive/tuberia/actions/workflows/cicd.yaml/badge.svg"
             alt="Tuberia CI pipeline status">
    </a>
    <a href="https://app.codecov.io/gh/AIdictive/tuberia/" target="_blank">
        <img src="https://img.shields.io/codecov/c/github/aidictive/tuberia"
             alt="Tuberia coverage status">
    </a>
    <a href="https://github.com/AIdictive/tuberia/issues" target="_blank">
        <img src="https://img.shields.io/bitbucket/issues/AIdictive/tuberia"
             alt="Tuberia issues">
    </a>
    <a href="https://github.com/aidictive/tuberia/graphs/contributors" target="_blank">
        <img src="https://img.shields.io/github/contributors/AIdictive/tuberia"
             alt="Tuberia contributors">
    </a>
    <a href="https://pypi.org/project/tuberia/" target="_blank">
        <img src="https://pepy.tech/badge/tuberia"
             alt="Tuberia total downloads">
    </a>
    <a href="https://pypi.org/project/tuberia/" target="_blank">
        <img src="https://pepy.tech/badge/tuberia/month"
             alt="Tuberia downloads per month">
    </a>
    <br />
    Data engineering meets software engineering
</p>

---

:books: **Documentation**:
<a href="https://aidictive.github.io/tuberia" target="_blank">
    https://aidictive.github.io/tuberia
</a>

:keyboard: **Source Code**:
<a href="https://github.com/aidictive/tuberia" target="_blank">
    https://github.com/aidictive/tuberia
</a>

---


## :thinking_face: What is this?

Tuberia is born from the need to bring the worlds of data and software
engineering closer together. Here is a list of common problems in data
projects:

* Loooooong SQL queries impossible to understand/test.
* A lot of duplicate code due to the difficulty of reusing it in SQL queries.
* Lack of tests, sometimes because the used framework does not facilitate
testing tasks.
* Lack of documentation.
* Discrepancies between the existing documentation and the latest deployed code.
* A set of notebooks deployed under the Databricks Share folder.
* A generic notebook with utility functions.
* Use of drag-and-drop frameworks that limit the developer's creativity.
* Months of intense work to migrate existing pipelines from one orchestrator to
another (e.g. from Airflow to Prefect, from Databricks Jobs to Data
Factory...).

Tuberia aims to solve all these problems and many others. 


## :nerd_face: How it works?

Programming gurus will enjoy data engineering again!



## Getting started

You need:

* Spark 3.2.
* Java JDK 11 (Required by Spark).
* [Poetry](https://python-poetry.org/docs/#installation).
* Make.

Once you have all the tools installed just open a shell on the root folder of
the project and install the dependencies in a new virtual environment with:

```sh
$ make install
```

The previous command also installs some [pre-commits](https://pre-commit.com).

Check that your package is installed with:

```sh
$ poetry run tuberia
▄▄▄█████▓ █    ██  ▄▄▄▄   ▓█████  ██▀███   ██▓ ▄▄▄
▓  ██▒ ▓▒ ██  ▓██▒▓█████▄ ▓█   ▀ ▓██ ▒ ██▒▓██▒▒████▄
▒ ▓██░ ▒░▓██  ▒██░▒██▒ ▄██▒███   ▓██ ░▄█ ▒▒██▒▒██  ▀█▄
░ ▓██▓ ░ ▓▓█  ░██░▒██░█▀  ▒▓█  ▄ ▒██▀▀█▄  ░██░░██▄▄▄▄██
  ▒██▒ ░ ▒▒█████▓ ░▓█  ▀█▓░▒████▒░██▓ ▒██▒░██░ ▓█   ▓██▒
  ▒ ░░   ░▒▓▒ ▒ ▒ ░▒▓███▀▒░░ ▒░ ░░ ▒▓ ░▒▓░░▓   ▒▒   ▓▒█░
    ░    ░░▒░ ░ ░ ▒░▒   ░  ░ ░  ░  ░▒ ░ ▒░ ▒ ░  ▒   ▒▒ ░
  ░       ░░░ ░ ░  ░    ░    ░     ░░   ░  ▒ ░  ░   ▒
            ░      ░         ░  ░   ░      ░        ░  ░
Version 0.0.0
```

If you can see that funky logo your installation is correct. Note that the
version may change.

If you do not want to use `poetry run` in front of all your commands just
activate the virtual environment with `poetry shell`. Use `exit` if you want to
deactivate the environment.


## How do I build the package?

You can build the package without installing the dependencies or without a
proper Spark installation. Use `make build` or just `make`. You should see
something like:

```sh
$ make
poetry build
Building tuberia (0.0.0)
  - Building sdist
  - Built tuberia-0.0.0.tar.gz
  - Building wheel
  - Built tuberia-0.0.0-py3-none-any.whl
```


## How do I run tests?

Run tests locally with:

```sh
$ make test
```


## Contribution guidelines

* The code is auto-formatted by Black, so you can write the code without
following any style guide and Black will take care of making it consistent
with the current codebase.
* Write tests: test not added in the PR, test that will never be added.
