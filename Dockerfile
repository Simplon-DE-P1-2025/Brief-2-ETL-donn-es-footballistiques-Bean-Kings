FROM python:3.12-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /etl

COPY etl /etl

CMD ["python", "main.py"]