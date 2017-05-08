.PHONY: build

build:
	python3 setup.py install

test:
	make typecheck
	pytest tests -v --cov=aw_core --cov=aw_datastore

benchmark:
	python -m aw_datastore.benchmark

typecheck:
	export MYPYPATH=./stubs; mypy aw_core aw_datastore --ignore-missing-imports --follow-imports=skip

typecheck-strict:
	export MYPYPATH=./stubs; mypy aw_core aw_datastore --strict-optional --check-untyped-defs; echo "Not a failing step"

