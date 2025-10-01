UV=.venv/bin/python

.PHONY: smoke
smoke:
	uv run --python .venv python tools/smoke_test_dashboard.py

.PHONY: smoke-cube
smoke-cube:
	uv run --python .venv python tools/smoke_test_dashboard.py --with-cube

