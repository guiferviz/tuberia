POETRY=poetry
PYTEST=$(POETRY) run pytest
PACKAGE={{ cookiecutter.module_name }}


build: requirements.txt
	$(POETRY) build

requirements.txt: pyproject.toml
	$(POETRY) export -f requirements.txt --output requirements.txt --without-hashes

publish: build
	$(POETRY) publish -r jfrog_local

install:
	$(POETRY) install -E pyspark
	$(POETRY) run pre-commit install

test:
	$(PYTEST) --cov=$(PACKAGE) tests/

doc:
	mkdocs build

serve-doc:
	mkdocs serve
