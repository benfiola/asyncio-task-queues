ARG PYTHON_VERSION=3.9
FROM python:${PYTHON_VERSION}
WORKDIR /app

COPY setup.py setup.py
COPY pyproject.toml pyproject.toml
COPY tests tests
COPY asyncio_task_queues asyncio_task_queues
RUN pip install ".[dev]"
RUN black --check .
RUN pytest tests
