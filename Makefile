# Local shortcuts only. CI calls the same tools directly.
.PHONY: setup lint fmt test image

IMAGE ?= jtf:local

setup:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

test:
	uv run pytest

image:
	docker build -t $(IMAGE) .
