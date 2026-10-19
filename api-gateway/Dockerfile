# Build from repo root:  docker build -f api-gateway/Dockerfile -t api-gateway .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY . .
RUN mvn -B -q -pl api-gateway -am -DskipTests package

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S -u 10001 -G app app
WORKDIR /app
COPY --from=build /workspace/api-gateway/target/*.jar app.jar
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseZGC -XX:+ZGenerational"
USER 10001
EXPOSE 8080 9090
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
