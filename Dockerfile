FROM amazoncorretto:8 AS builder
WORKDIR /app
COPY . /app
RUN ./gradlew --no-daemon assembleShadowDist

FROM amazoncorretto:8
WORKDIR /app
COPY --from=builder /app/build/libs/pitcher-indexer-0.1.0-all.jar .
COPY default.properties /app/
CMD ["java", "-jar", "pitcher-indexer-0.1.0-all.jar"]
