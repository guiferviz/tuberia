POETRY=poetry
PYTEST=$(POETRY) run pytest
GIT=git
GIT_TAG=$(shell $(GIT) describe --tags)


build: requirements.txt
	$(POETRY) build

requirements.txt: pyproject.toml
	poetry export -f requirements.txt --output requirements.txt --without-hashes

publish: build
ifdef PYPI_URL
	$(POETRY) config repositories.pypi $(PYPI_URL)
	$(POETRY) publish -r pypi -u __token__ -p $(PYPI_TOKEN) || $(POETRY) config repositories.pypi --unset
else
	$(POETRY) publish -u __token__ -p $(PYPI_TOKEN)
endif

install:
	$(POETRY) install -E pyspark
	$(POETRY) run pre-commit install

format_and_lint:
	$(POETRY) run pre-commit run --all

test:
	$(POETRY) run coverage run --source=src/ -m pytest tests/
	$(POETRY) run coverage report
	$(POETRY) run coverage xml
	$(POETRY) run coverage html

doc:
	$(POETRY) run mkdocs build

publish-doc:
	$(POETRY) version $(GIT_TAG)
	$(POETRY) run mkdocs gh-deploy --force

serve-doc:
	mkdocs serve
