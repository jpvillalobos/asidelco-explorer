FROM python:3.10-slim

WORKDIR /app
COPY onedrive_listener.py /app/onedrive_listener.py

RUN pip install --no-cache-dir msal azure-storage-blob pandas requests openpyxl

# Logs directory
RUN mkdir -p /app/logs
VOLUME ["/app/logs"]

ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/onedrive_listener.py"]