<p align="center">
    <img src="docs/images/logo.png" width="800">
</p>

<p align="center">
    <a href="https://github.com/AIdictive/tuberia/actions/workflows/cicd.yaml">
        <img src="https://github.com/aidictive/tuberia/actions/workflows/cicd.yaml/badge.svg" alt="Tuberia CI pipeline status">
    </a>
    <a href="https://app.codecov.io/gh/AIdictive/tuberia/">
        <img src="https://img.shields.io/codecov/c/github/aidictive/tuberia" alt="Tuberia coverage status">
    </a>
    <a href="https://github.com/AIdictive/tuberia/issues">
        <img src="https://img.shields.io/bitbucket/issues/AIdictive/tuberia" alt="Tuberia issues">
    </a>
    <img src="https://img.shields.io/github/contributors/AIdictive/tuberia" alt="Tuberia contributors">
    <img src="https://pepy.tech/badge/tuberia" alt="Tuberia total downloads">
    <img src="https://pepy.tech/badge/tuberia/month" alt="Tuberia downloads per month">
    <br />
    Data engineering meets software engineering
</p>

---

**Documentation**: <a href="https://aidictive.github.io/tuberia" target="_blank">https://aidictive.github.io/tuberia</a>

**Source Code**: <a href="https://github.com/aidictive/tuberia" target="_blank">https://github.com/aidictive/tuberia</a>

---


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
