.PHONY: build test benchmark typecheck typecheck-strict clean

build:
	poetry install

test:
	python -m pytest tests -v --cov=aw_core --cov=aw_datastore --cov=aw_transform --cov=aw_query

.coverage:
	make test

coverage_html: .coverage
	python -m coverage html -d coverage_html

benchmark:
	python -m aw_datastore.benchmark

typecheck:
	export MYPYPATH=./stubs; python -m mypy aw_core aw_datastore aw_transform aw_query --show-traceback --ignore-missing-imports --follow-imports=skip

typecheck-strict:
	export MYPYPATH=./stubs; python -m mypy aw_core aw_datastore aw_transform aw_query --strict-optional --check-untyped-defs; echo "Not a failing step"

PYFILES=$(shell find . -type f -name '*.py')

lint-fix:
	pyupgrade --py37-plus ${PYFILES} && true
	black ${PYFILES}

clean:
	rm -rf build dist
	rm -rf aw_core/__pycache__ aw_datastore/__pycache__
