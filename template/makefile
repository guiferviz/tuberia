BAKE_OPTIONS=--no-input

bake:
	poetry run cookiecutter $(BAKE_OPTIONS) . --overwrite-if-exists

install:
	poetry install

test:
	poetry run pytest

help:
	@echo "Makefile help"
	@echo "-------------"
	@echo "bake 	generate project using defaults"
	@echo " 	same as bake"
	@echo "watch 	generate project using defaults and watch for changes"
	@echo "replay 	replay last cookiecutter run and watch for changes"

watch: bake
	poetry run watchmedo shell-command -p '*.*' -c 'make bake -e BAKE_OPTIONS=$(BAKE_OPTIONS)' -W -R -D \{{cookiecutter.project_slug}}/

replay: BAKE_OPTIONS=--replay
replay: watch
	;
