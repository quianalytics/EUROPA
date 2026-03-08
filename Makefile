.PHONY: run run-debug install install-dev install-reqs

PYTHON := .venv/bin/python
PIP := .venv/bin/pip
HOST ?= 127.0.0.1
PORT ?= 5000

run:
	@echo "Starting WarRoom backend on http://$(HOST):$(PORT)"
	@$(PYTHON) app.py

run-debug:
	@echo "Starting WarRoom backend (debug) on http://$(HOST):$(PORT)"
	@DEBUG=true HOST=$(HOST) PORT=$(PORT) $(PYTHON) app.py

install:
	@$(PIP) install -r requirements.txt

install-dev:
	@$(PIP) install -r requirements.txt
