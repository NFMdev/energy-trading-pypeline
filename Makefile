.PHONY: help install test lint format typecheck check up down restart logs check-db produce consume clean

help:
	@echo "Energy Trading Pypeline - Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "	make install	Install project dependencies with uv"
	@echo ""
	@echo "Quality:"
	@echo "	make test	Run test suite"
	@echo "	make lint	Run ruff linting"
	@echo "	make format	Format code with ruff"
	@echo "	make typecheck	Run mypy strict checks"
	@echo "	make check	Run lint, typecheck, and tests"
	@echo ""
	@echo "Infrastructure:"
	@echo "	make up		Start local Docker Compose infrastructure"
	@echo "	make down	Stop local Docker Compose infrastructure"
	@echo "	make restart	Restart local Docker Compose infrastructure"
	@echo "	make logs	Follow Docker Compose logs"
	@echo ""
	@echo "Pipeline:"
	@echo "	make check-db	Check PostgreSQL connectivity"
	@echo "	make produce	Produce synthetic events to Redpanda/Kafka"
	@echo "	make consume	Consume raw events and persist them"
	@echo ""
	@echo "Maintenance:"
	@echo "	make clean	Remove local Python cache files"

install:
	uv sync

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .
	uv run ruff check . --fix

typecheck:
	uv run mypy src tests

check: lint typecheck test

up:
	docker compose up -d

down: docker compose down

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