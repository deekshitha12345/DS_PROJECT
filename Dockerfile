FROM python:3.10-slim

WORKDIR /app

COPY pyproject.toml README.md pytest.ini ./
COPY distributed_cache ./distributed_cache
COPY tests ./tests
COPY scripts ./scripts

RUN pip install --no-cache-dir -e .

EXPOSE 8000

CMD ["python", "-m", "distributed_cache.entrypoint"]
