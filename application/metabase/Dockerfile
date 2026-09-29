FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY setup_dashboards.py .
CMD ["python", "setup_dashboards.py"]
