# JVM image for the parse task: java ReplayInfo <replay.dem> <out.json>
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
# strip CRLF in case git checked gradlew out with Windows line endings
RUN sed -i 's/\r$//' gradlew && chmod +x gradlew && ./gradlew installDist --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=build /src/build/install/parser /app
ENTRYPOINT ["java", "-cp", "/app/lib/*", "ReplayInfo"]
