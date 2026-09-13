# Development tasks. `just` with no arguments lists them.

default:
    @just --list

lint:
    .venv/bin/ruff check erpl_dlt tests
    .venv/bin/ruff format --check erpl_dlt tests

format:
    .venv/bin/ruff format erpl_dlt tests
    .venv/bin/ruff check --fix erpl_dlt tests

typecheck:
    .venv/bin/mypy

# Unit tests: no SAP system needed.
test:
    .venv/bin/python -m pytest tests -q -m "not integration"

# Integration tests: a real SAP system, no mocks.
#   ERPL_IT=1 ERPL_SAP_PASSWORD=... just it
it:
    ERPL_IT=1 .venv/bin/python -m pytest tests -q

check: lint typecheck test
