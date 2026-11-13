# Built by docker compose (context: dashboard/). Settings come from the WAREHOUSE_* variables in docker-compose.yml.
FROM python:3.14.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
RUN useradd --create-home dashboard
USER dashboard
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]
