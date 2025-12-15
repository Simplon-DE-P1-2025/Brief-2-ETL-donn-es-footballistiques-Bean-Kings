FROM python:3.12-slim

RUN pip install --no-cache-dir duckdb

WORKDIR /etl

COPY etl /etl

CMD ["bash"]