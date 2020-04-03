FROM python:3.8-buster
ENV POETRY_VERSION=1.0.3

RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /app

COPY pyproject.toml poetry.toml poetry.lock ./
RUN poetry install --no-dev --no-root

COPY redis_data_transfer ./redis_data_transfer
RUN poetry install --no-dev

ENTRYPOINT ["poetry", "run", "redis-data-transfer"]
