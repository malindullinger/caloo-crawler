SHELL := /bin/bash

# Use the venv python if present
PY := .venv/bin/python
PIP := .venv/bin/pip

.DEFAULT_GOAL := help

help:
	@echo ""
	@echo "Caloo-crawler commands"
	@echo "----------------------"
	@echo "make venv        Create venv + install deps"
	@echo "make deps        Install/update deps (requires venv)"
	@echo "make check       Quick sanity checks (import + simple query)"
	@echo "make run         Run pipeline module (override with MODULE=...)"
	@echo "make fmt         Format code (if formatter installed)"
	@echo "make clean       Remove caches"
	@echo ""

venv:
	@test -d .venv || python3 -m venv .venv
	@$(PIP) install -U pip
	@if [ -f requirements.txt ]; then $(PIP) install -r requirements.txt; \
	else echo "No requirements.txt found (ok)"; fi

deps:
	@$(PIP) install -U pip
	@if [ -f requirements.txt ]; then $(PIP) install -r requirements.txt; \
	else echo "No requirements.txt found (ok)"; fi

check:
	@$(PY) -c "from src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY; print('OK config loaded')"
	@$(PY) -c "from src.storage import supabase; print('OK supabase client created')"
	@$(PY) -c "from src.storage import supabase; r=supabase.table('events').select('external_id').limit(1).execute(); print('OK query', len(r.data) if getattr(r,'data',None) is not None else 0)"

# Default pipeline module (override: make run MODULE=src.some_other_entry)
MODULE ?= src.pipeline

run:
	@$(PY) -c "import importlib; importlib.import_module('$(MODULE)'); print('OK module import:', '$(MODULE)')" >/dev/null
	@$(PY) -m $(MODULE)

fmt:
	@echo "Optional: install ruff/black and wire formatting here"

clean:
	@find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	@find . -name "*.pyc" -delete
