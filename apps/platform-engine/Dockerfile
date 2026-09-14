# Stage 1: Build the application
FROM gradle:8-jdk21-alpine AS builder
WORKDIR /app
COPY build.gradle .
COPY settings.gradle .

# Download dependencies (this caches them if gradle files don't change)
RUN gradle dependencies --no-daemon || true

COPY src src
RUN gradle bootJar --no-daemon

# Stage 2: Run the application
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Create a non-root user
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

# Copy the built artifact from builder
COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
