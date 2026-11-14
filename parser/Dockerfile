# Parse image: PySpark (local mode) + the Java replay extractor + s3a for Garage.
# Build from the repo root (needs parser/ and spark_jobs/):
#   docker build -t dota-parser -f parser/Dockerfile .
# Run: docker run dota-parser <replays glob or comma list> <out dir>   (see spark_jobs/parse_replays.py)
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY parser/ .
# strip CRLF in case git checked gradlew out with Windows line endings
RUN sed -i 's/\r$//' gradlew && chmod +x gradlew && ./gradlew extractorTests extractorJars s3aJars --no-daemon

FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*
COPY spark_jobs/requirements.txt /app/spark_jobs/requirements.txt
RUN pip install --no-cache-dir -r /app/spark_jobs/requirements.txt
COPY --from=build /src/build/extractor /src/build/s3a /app/jars/
COPY spark_jobs/ /app/spark_jobs/
ENV EXTRACTOR_JARS=/app/jars
WORKDIR /app/spark_jobs
ENTRYPOINT ["python", "parse_replays.py"]
