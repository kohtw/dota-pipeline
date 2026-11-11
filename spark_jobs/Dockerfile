# Build from the repository root: docker build -t dota-spark-clean -f spark_jobs/Dockerfile .
FROM eclipse-temurin:21-jdk AS jars
WORKDIR /src
COPY parser/ .
RUN sed -i 's/\r$//' gradlew && chmod +x gradlew && ./gradlew s3aJars --no-daemon

FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*
COPY spark_jobs/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --from=jars /src/build/s3a /app/jars
COPY spark_jobs/clean_replays.py /app/clean_replays.py
ENV EXTRACTOR_JARS=/app/jars
ENTRYPOINT ["python", "/app/clean_replays.py"]
