.PHONY: help install test test-unit test-integration test-all lint format typecheck check check-all up down restart logs check-db produce consume clean precommit-install precommit

help:
	@echo "Energy Trading Pypeline - Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "	make install		Install project dependencies with uv"
	@echo "	make precommit-install	Install Git pre-commit hooks"
	@echo ""
	@echo "Quality:"
	@echo "	make test		Run unit tests"
	@echo "	make test-unit		Run unit tests"
	@echo "	make test-integration	Run integration tests if available"
	@echo "	make test-all		Run test suite"
	@echo "	make lint		Run ruff linting"
	@echo "	make format		Format code with ruff"
	@echo "	make typecheck		Run mypy strict checks"
	@echo "	make check		Run lint, typecheck, and unit tests"
	@echo "	make check-all		Run lint, typecheck, and test suite"
	@echo "	make precommit		Run pre-commit hooks on all files"
	@echo ""
	@echo "Infrastructure:"
	@echo "	make up			Start local Docker Compose infrastructure"
	@echo "	make down		Stop local Docker Compose infrastructure"
	@echo "	make restart		Restart local Docker Compose infrastructure"
	@echo "	make logs		Follow Docker Compose logs"
	@echo ""
	@echo "Pipeline:"
	@echo "	make check-db		Check PostgreSQL connectivity"
	@echo "	make produce		Produce synthetic events to Redpanda/Kafka"
	@echo "	make consume		Consume raw events and persist them"
	@echo ""
	@echo "Maintenance:"
	@echo "	make clean		Remove local Python cache files"

install:
	uv sync

test: test-unit

test-unit:
	uv run pytest tests/unit

test-integration:
	@if find tests/integration -type f \( -name "test_*.py" -o -name "*_test.py" \) | grep -q .; then \
		uv run pytest tests/integration; \
	else \
		echo "No integration tests found."; \
	fi

test-all: test-unit test-integration

lint:
	uv run ruff check .

format:
	uv run ruff format .
	uv run ruff check . --fix

typecheck:
	uv run mypy src tests

check: lint typecheck test-unit

check-all: lint typecheck test-all

up:
	docker compose up -d

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

check-db:
	uv run energy-check-db

produce:
	uv run energy-produce

consume:
	uv run energy-consume

clean:
	find . -type d -name "__pycache__" -prune exec rm -rf {} +
	find . -type d -name ".pytest_cache" -prune exec rm -rf {} +
	find . -type d -name ".mypy_cache" -prune exec rm -rf {} +
	find . -type d -name ".ruff_cache" -prune exec rm -rf {} +

precommit-install:
	uv run pre-commit install

precommit:
	uv run pre-commit run --all-files
