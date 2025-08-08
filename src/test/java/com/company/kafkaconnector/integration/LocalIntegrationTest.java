package com.company.kafkaconnector.integration;

import com.company.kafkaconnector.connector.S3DeltaSinkConnector;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Local integration tests using Podman containers (RedPanda + MinIO)
 * These tests require the containers to be running locally
 */
@SpringBootTest
@ActiveProfiles("test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = "true")
public class LocalIntegrationTest {
    
    private static final Logger log = LoggerFactory.getLogger(LocalIntegrationTest.class);
    
    private static final String KAFKA_HOST = "localhost";
    private static final int KAFKA_PORT = 9092;
    private static final String MINIO_HOST = "localhost";
    private static final int MINIO_PORT = 9000;
    private static final String CONNECT_WORKER1_HOST = "localhost";
    private static final int CONNECT_WORKER1_PORT = 8083;
    private static final String CONNECT_WORKER2_HOST = "localhost";
    private static final int CONNECT_WORKER2_PORT = 8084;
    
    private static HttpClient httpClient;
    
    @BeforeAll
    static void setUp() {
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        log.info("Local integration test setup completed");
    }
    
    @Test
    @Order(1)
    @DisplayName("Test 1: Verify prerequisites - Kafka, MinIO containers")
    void testPrerequisites() {
        log.info("Checking prerequisites for local integration test");
        
        // Check Kafka/RedPanda
        assertThat(isPortOpen(KAFKA_HOST, KAFKA_PORT))
                .describedAs("Kafka/RedPanda should be running on %s:%d", KAFKA_HOST, KAFKA_PORT)
                .isTrue();
        
        // Check MinIO
        assertThat(isPortOpen(MINIO_HOST, MINIO_PORT))
                .describedAs("MinIO should be running on %s:%d", MINIO_HOST, MINIO_PORT)
                .isTrue();
        
        log.info("✅ Prerequisites verified: Kafka and MinIO are running");
    }
    
    @Test
    @Order(2)
    @DisplayName("Test 2: Verify connector class and configuration")
    void testConnectorConfiguration() {
        log.info("Testing connector configuration");
        
        S3DeltaSinkConnector connector = new S3DeltaSinkConnector();
        
        // Test version
        String version = connector.version();
        assertThat(version).isEqualTo("1.0.0");
        
        // Test task class
        assertThat(connector.taskClass().getSimpleName()).isEqualTo("S3DeltaSinkTask");
        
        // Test configuration definition
        assertThat(connector.config()).isNotNull();
        assertThat(connector.config().configKeys()).isNotEmpty();
        
        log.info("✅ Connector configuration validated");
    }
    
    @Test
    @Order(3)
    @DisplayName("Test 3: Test task configuration generation for scaling")
    void testTaskConfigurationScaling() {
        log.info("Testing task configuration for horizontal scaling");
        
        S3DeltaSinkConnector connector = new S3DeltaSinkConnector();
        
        // Test configuration for scaling
        Map<String, String> props = Map.of(
                "name", "test-connector",
                "s3.bucket.name", "test-bucket",
                "topics", "test-topic"
        );
        
        connector.start(props);
        
        // Test with different task counts
        var taskConfigs1 = connector.taskConfigs(1);
        assertThat(taskConfigs1).hasSize(1);
        
        var taskConfigs4 = connector.taskConfigs(4);
        assertThat(taskConfigs4).hasSize(4);
        
        // Verify each task has unique ID
        for (int i = 0; i < taskConfigs4.size(); i++) {
            assertThat(taskConfigs4.get(i).get("task.id")).isEqualTo(String.valueOf(i));
        }
        
        connector.stop();
        log.info("✅ Task configuration scaling validated");
    }
    
    @Test
    @Order(4)
    @DisplayName("Test 4: Check if Kafka Connect workers are running (optional)")
    void testKafkaConnectWorkers() {
        log.info("Checking if Kafka Connect workers are running");
        
        boolean worker1Running = isPortOpen(CONNECT_WORKER1_HOST, CONNECT_WORKER1_PORT);
        boolean worker2Running = isPortOpen(CONNECT_WORKER2_HOST, CONNECT_WORKER2_PORT);
        
        if (worker1Running) {
            log.info("✅ Kafka Connect Worker 1 detected on port {}", CONNECT_WORKER1_PORT);
            testWorkerEndpoint("http://localhost:" + CONNECT_WORKER1_PORT);
        } else {
            log.info("⚠️  Kafka Connect Worker 1 not running on port {}", CONNECT_WORKER1_PORT);
        }
        
        if (worker2Running) {
            log.info("✅ Kafka Connect Worker 2 detected on port {}", CONNECT_WORKER2_PORT);
            testWorkerEndpoint("http://localhost:" + CONNECT_WORKER2_PORT);
        } else {
            log.info("⚠️  Kafka Connect Worker 2 not running on port {}", CONNECT_WORKER2_PORT);
        }
        
        if (worker1Running && worker2Running) {
            log.info("🚀 Both workers running - horizontal scaling can be tested");
        } else {
            log.info("ℹ️  To test horizontal scaling, run:");
            log.info("   Terminal 1: ./scripts/run-distributed-worker1.sh");
            log.info("   Terminal 2: ./scripts/run-distributed-worker2.sh");
        }
    }
    
    @Test
    @Order(5)
    @DisplayName("Test 5: Instructions for manual horizontal scaling test")
    void testHorizontalScalingInstructions() {
        log.info("Manual horizontal scaling test instructions");
        
        Map<String, String> instructions = Map.of(
                "Step 1", "Ensure RedPanda and MinIO containers are running",
                "Step 2", "Compile connector: mvn clean compile",
                "Step 3", "Run scaling test: ./scripts/test-horizontal-scaling.sh",
                "Step 4", "Monitor workers: http://localhost:8083 and http://localhost:8084",
                "Step 5", "Check metrics: curl http://localhost:8083/actuator/connector/metrics",
                "Step 6", "Verify S3 files: MinIO console at http://localhost:9001"
        );
        
        instructions.forEach((step, instruction) -> {
            log.info("{}: {}", step, instruction);
        });
        
        // Expected results
        Map<String, String> expectedResults = Map.of(
                "Task Distribution", "4 tasks split across 2 workers (2 tasks each)",
                "Throughput Improvement", "60-80% increase with 2 workers",
                "Failover Time", "< 30 seconds for task rebalancing",
                "Data Integrity", "No duplicate S3 files or lost messages",
                "Metrics", "Both workers report processing statistics"
        );
        
        log.info("Expected Results:");
        expectedResults.forEach((metric, expected) -> {
            log.info("  {}: {}", metric, expected);
        });
        
        log.info("✅ Manual test instructions provided");
    }
    
    private boolean isPortOpen(String host, int port) {
        try (Socket socket = new Socket(host, port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
    
    private void testWorkerEndpoint(String baseUrl) {
        try {
            // Test root endpoint
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl))
                    .GET()
                    .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == 200) {
                log.info("  ✅ Worker REST API responding at {}", baseUrl);
                
                // Test connectors endpoint
                HttpRequest connectorsRequest = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/connectors"))
                        .GET()
                        .build();
                
                HttpResponse<String> connectorsResponse = httpClient.send(connectorsRequest, 
                        HttpResponse.BodyHandlers.ofString());
                
                if (connectorsResponse.statusCode() == 200) {
                    log.info("  ✅ Connectors endpoint available: {}", connectorsResponse.body());
                }
            } else {
                log.warn("  ⚠️  Worker endpoint returned status: {}", response.statusCode());
            }
            
        } catch (Exception e) {
            log.warn("  ⚠️  Failed to test worker endpoint {}: {}", baseUrl, e.getMessage());
        }
    }
}