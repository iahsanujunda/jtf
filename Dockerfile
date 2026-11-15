# One image for every role: the jtf subcommand picks the step.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Dependencies first, so code changes don't invalidate this layer.
COPY pyproject.toml uv.lock .python-version ./
COPY jtf/pyproject.toml jtf/pyproject.toml
RUN uv sync --frozen --no-dev --package jtf --no-install-workspace

COPY jtf/ jtf/
RUN uv sync --frozen --no-dev --package jtf

RUN useradd --create-home --uid 10001 jtf
USER jtf

ENTRYPOINT ["jtf"]
