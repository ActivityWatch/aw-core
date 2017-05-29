.PHONY: build

build:
	python3 setup.py install

test:
	make typecheck
	python3 -m pytest tests -v --cov=aw_core --cov=aw_datastore

benchmark:
	python3 -m aw_datastore.benchmark

typecheck:
	# This first line is just for testing if my appveyor python3 copy messes something up
	export MYPYPATH=./stubs; python -m mypy aw_core aw_datastore --show-traceback --ignore-missing-imports --follow-imports=skip
	export MYPYPATH=./stubs; python3 -m mypy aw_core aw_datastore --show-traceback --ignore-missing-imports --follow-imports=skip

typecheck-strict:
	export MYPYPATH=./stubs; python3 -m mypy aw_core aw_datastore --strict-optional --check-untyped-defs; echo "Not a failing step"

