FROM maven:3.8.6-openjdk-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

COPY src/main/resources/certs/root.crt /app/root.crt

FROM openjdk:21-jdk-slim
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
COPY --from=build /app/root.crt /app/root.crt  # ← Копируем из build стадии

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]