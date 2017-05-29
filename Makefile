.PHONY: build

build:
	python3 setup.py install

test:
	make typecheck
	python3 -m pytest tests -v --cov=aw_core --cov=aw_datastore

benchmark:
	python3 -m aw_datastore.benchmark

typecheck:
	python3 -m mypy aw_core aw_datastore --show-traceback --ignore-missing-imports --follow-imports=skip

typecheck-strict:
	export MYPYPATH=./stubs; python3 -m mypy aw_core aw_datastore --strict-optional --check-untyped-defs; echo "Not a failing step"

