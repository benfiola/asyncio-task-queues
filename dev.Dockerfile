ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}
ARG MODE
WORKDIR /app

COPY setup.py setup.py
COPY pyproject.toml pyproject.toml
COPY asyncio_task_queues asyncio_task_queues
RUN pip install ".[dev]"

COPY tests tests
COPY docs docs
COPY mkdocs.yml mkdocs.yml
COPY README.md README.md
COPY dev.entrypoint.sh dev.entrypoint.sh

ENTRYPOINT [ "./dev.entrypoint.sh" ]