test::
	pytest -v tests

format::
	ruff check --fix src
build:: format
	uv build

publish:: build
	sh -c 'uv publish --token "$$PYPI_API_TOKEN"'