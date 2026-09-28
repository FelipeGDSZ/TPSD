FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements-runtime.txt ./
RUN pip install --no-cache-dir -r requirements-runtime.txt

COPY *.py ./
COPY templates/ ./templates/

RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

ENTRYPOINT ["python"]
CMD ["dashboard.py"]
