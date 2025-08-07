# Multi-stage build for Kafka S3 Delta Connector
# Production-ready Dockerfile with optimized layers and security

# Build stage
FROM eclipse-temurin:17-jdk-alpine AS builder

# Set working directory
WORKDIR /app

# Copy Maven configuration files first (for better layer caching)
COPY pom.xml ./
COPY .mvn .mvn
COPY mvnw ./

# Make Maven wrapper executable
RUN chmod +x mvnw

# Download dependencies (cached layer)
RUN ./mvnw dependency:go-offline -B

# Copy source code
COPY src ./src
COPY config ./config

# Build the application
RUN ./mvnw clean package -DskipTests -B

# Runtime stage
FROM eclipse-temurin:17-jre-alpine AS runtime

# Install necessary packages and create non-root user
RUN apk add --no-cache \
    curl \
    ca-certificates \
    tzdata \
    && addgroup -g 1001 -S appgroup \
    && adduser -u 1001 -S appuser -G appgroup

# Set timezone
ENV TZ=UTC

# Create application directories
RUN mkdir -p /app/config /app/logs /app/data \
    && chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Set working directory
WORKDIR /app

# Copy JAR from build stage
COPY --from=builder --chown=appuser:appgroup /app/target/kafka-s3-connector-*.jar app.jar

# Copy configuration files
COPY --from=builder --chown=appuser:appgroup /app/config/ ./config/

# Expose ports
EXPOSE 8083 8084 8081

# JVM optimization for containers
ENV JAVA_OPTS="-XX:+UseContainerSupport \
               -XX:MaxRAMPercentage=75.0 \
               -XX:+UseG1GC \
               -XX:+UseStringDeduplication \
               -XX:+OptimizeStringConcat \
               -XX:+UnlockExperimentalVMOptions \
               -XX:+UseCGroupMemoryLimitForHeap \
               -Djava.security.egd=file:/dev/./urandom \
               -Dspring.profiles.active=prod"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8081/actuator/health || exit 1

# Application metadata
LABEL org.opencontainers.image.title="Kafka S3 Delta Connector"
LABEL org.opencontainers.image.description="Enterprise Kafka Connect S3 Delta Lake Connector with horizontal scaling"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.source="https://github.com/company/kafka-s3-connector"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.vendor="Company Engineering"

# Runtime command
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]

# Default command arguments
CMD ["--spring.config.location=classpath:/,file:./config/"]