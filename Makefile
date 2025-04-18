test::
	pytest -v tests

format::
	ruff check --fix src
build:: format
	uv build

publish:: build
	uv publish 