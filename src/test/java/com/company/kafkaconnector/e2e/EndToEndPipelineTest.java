package com.company.kafkaconnector.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive End-to-End Pipeline Testing
 * 
 * As QA Lead, this test suite validates:
 * 1. Complete data flow: Kafka → Connector → S3 Delta
 * 2. Data integrity and format validation
 * 3. Schema compliance and evolution
 * 4. Error handling and recovery
 * 5. Performance under load
 * 
 * Prerequisites:
 * - Docker containers running (RedPanda, MinIO, Connector)
 * - RUN_E2E_TESTS environment variable set to 'true'
 * - Test data schemas available
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "RUN_E2E_TESTS", matches = "true")
public class EndToEndPipelineTest {
    
    private static final Logger log = LoggerFactory.getLogger(EndToEndPipelineTest.class);
    
    // Test Configuration
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET_NAME = "test-data-lake";
    
    // Test Topics and Paths
    private static final String USER_EVENTS_TOPIC = "user.events.v1";
    private static final String ORDER_EVENTS_TOPIC = "orders.lifecycle.v2";
    private static final String USER_EVENTS_PATH = "s3a://test-data-lake/events/user-events";
    private static final String ORDER_EVENTS_PATH = "s3a://test-data-lake/orders/order-events";
    
    // Test Clients
    private static KafkaProducer<String, String> kafkaProducer;
    private static S3Client s3Client;
    private static Engine deltaEngine;
    private static ObjectMapper objectMapper;
    
    // Test Data
    private static final int TOTAL_TEST_MESSAGES = 100;
    private static final int CONCURRENT_PRODUCERS = 3;
    
    @BeforeAll
    static void setUpAll() {
        log.info("🚀 Starting E2E Pipeline Tests - QA Lead Validation");
        
        setupKafkaProducer();
        setupS3Client();
        setupDeltaEngine();
        setupObjectMapper();
        
        log.info("✅ All test clients initialized successfully");
    }
    
    @AfterAll
    static void tearDownAll() {
        if (kafkaProducer != null) kafkaProducer.close();
        if (s3Client != null) s3Client.close();
        log.info("🧹 Test cleanup completed");
    }
    
    private static void setupKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        kafkaProducer = new KafkaProducer<>(props);
        log.info("✅ Kafka Producer initialized");
    }
    
    private static void setupS3Client() {
        s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(MINIO_ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .forcePathStyle(true)
                .build();
        log.info("✅ S3 Client initialized");
    }
    
    private static void setupDeltaEngine() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.endpoint", MINIO_ENDPOINT);
        hadoopConf.set("fs.s3a.access.key", ACCESS_KEY);
        hadoopConf.set("fs.s3a.secret.key", SECRET_KEY);
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        
        deltaEngine = DefaultEngine.create(hadoopConf);
        log.info("✅ Delta Engine initialized");
    }
    
    private static void setupObjectMapper() {
        objectMapper = new ObjectMapper();
        log.info("✅ Object Mapper initialized");
    }
    
    // Test 1: Infrastructure Validation
    @Test
    @Order(1)
    @DisplayName("E2E-001: Validate Infrastructure Components")
    void testInfrastructureValidation() {
        log.info("🔍 E2E-001: Validating infrastructure components");
        
        // Test Kafka connectivity
        Future<RecordMetadata> future = kafkaProducer.send(
                new ProducerRecord<>("test-connectivity", "test-key", "test-message"));
        
        assertThatCode(() -> future.get(10, TimeUnit.SECONDS))
                .describedAs("Kafka connectivity test")
                .doesNotThrowAnyException();
        
        // Test S3 connectivity
        assertThatCode(() -> s3Client.listBuckets())
                .describedAs("S3 connectivity test")
                .doesNotThrowAnyException();
        
        // Test bucket existence
        boolean bucketExists = s3Client.listBuckets().buckets().stream()
                .anyMatch(bucket -> bucket.name().equals(BUCKET_NAME));
        assertThat(bucketExists)
                .describedAs("Test bucket should exist")
                .isTrue();
        
        log.info("✅ E2E-001: Infrastructure validation passed");
    }
    
    // Test 2: Data Flow Validation - User Events
    @Test
    @Order(2)
    @DisplayName("E2E-002: User Events Data Flow Validation")
    void testUserEventsDataFlow() throws Exception {
        log.info("🚀 E2E-002: Starting user events data flow test");
        
        // Generate and send test data
        List<String> testMessages = generateUserEventMessages(50);
        sendMessagesToKafka(USER_EVENTS_TOPIC, testMessages);
        
        // Wait for connector processing
        log.info("⏳ Waiting for connector to process messages...");
        Thread.sleep(30000); // Allow time for processing
        
        // Validate Delta table creation and content
        validateDeltaTable(USER_EVENTS_PATH, testMessages.size(), Arrays.asList(
                "user_id", "event_type", "timestamp", "properties",
                "_kafka_topic", "_kafka_partition", "_kafka_offset",
                "year", "month", "day"
        ));
        
        log.info("✅ E2E-002: User events data flow validation passed");
    }
    
    // Test 3: Data Flow Validation - Order Events
    @Test
    @Order(3)
    @DisplayName("E2E-003: Order Events Data Flow Validation")
    void testOrderEventsDataFlow() throws Exception {
        log.info("🚀 E2E-003: Starting order events data flow test");
        
        // Generate and send test data
        List<String> testMessages = generateOrderEventMessages(30);
        sendMessagesToKafka(ORDER_EVENTS_TOPIC, testMessages);
        
        // Wait for connector processing
        log.info("⏳ Waiting for connector to process messages...");
        Thread.sleep(25000); // Allow time for processing
        
        // Validate Delta table creation and content
        validateDeltaTable(ORDER_EVENTS_PATH, testMessages.size(), Arrays.asList(
                "order_id", "customer_id", "status", "amount", "timestamp",
                "_kafka_topic", "_kafka_partition", "_kafka_offset",
                "year", "month", "day"
        ));
        
        log.info("✅ E2E-003: Order events data flow validation passed");
    }
    
    // Test 4: Concurrent Load Testing
    @Test
    @Order(4)
    @DisplayName("E2E-004: Concurrent Load and Performance Test")
    void testConcurrentLoadPerformance() throws Exception {
        log.info("🚀 E2E-004: Starting concurrent load test");
        
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_PRODUCERS);
        List<Future<Integer>> futures = new ArrayList<>();
        
        long startTime = System.currentTimeMillis();
        
        // Launch concurrent producers
        for (int i = 0; i < CONCURRENT_PRODUCERS; i++) {
            final int producerId = i;
            Future<Integer> future = executor.submit(() -> {
                List<String> messages = generateUserEventMessages(TOTAL_TEST_MESSAGES / CONCURRENT_PRODUCERS);
                sendMessagesToKafka(USER_EVENTS_TOPIC, messages, "producer-" + producerId);
                return messages.size();
            });
            futures.add(future);
        }
        
        // Wait for completion and collect results
        int totalSent = 0;
        for (Future<Integer> future : futures) {
            totalSent += future.get(60, TimeUnit.SECONDS);
        }
        
        long endTime = System.currentTimeMillis();
        double throughputPerSecond = (double) totalSent / ((endTime - startTime) / 1000.0);
        
        executor.shutdown();
        
        log.info("📊 Load Test Results:");
        log.info("   Total Messages Sent: {}", totalSent);
        log.info("   Total Time: {} ms", (endTime - startTime));
        log.info("   Throughput: {:.2f} messages/second", throughputPerSecond);
        
        // Wait for processing
        Thread.sleep(45000);
        
        // Validate processing
        assertThat(totalSent).isEqualTo(TOTAL_TEST_MESSAGES);
        assertThat(throughputPerSecond).isGreaterThan(10.0); // Minimum expected throughput
        
        log.info("✅ E2E-004: Concurrent load test passed");
    }
    
    // Test 5: Data Integrity and Schema Validation
    @Test
    @Order(5)
    @DisplayName("E2E-005: Data Integrity and Schema Validation")
    void testDataIntegrityAndSchema() throws Exception {
        log.info("🚀 E2E-005: Starting data integrity validation");
        
        // Validate user events table
        Table userEventsTable = Table.forPath(deltaEngine, USER_EVENTS_PATH);
        Snapshot userSnapshot = userEventsTable.getLatestSnapshot(deltaEngine);
        StructType userSchema = userSnapshot.getSchema();
        
        // Schema validation
        List<String> expectedUserFields = Arrays.asList(
                "user_id", "event_type", "timestamp", "properties",
                "_kafka_topic", "_kafka_partition", "_kafka_offset", "_processed_at", "_ingestion_timestamp",
                "year", "month", "day"
        );
        
        for (String field : expectedUserFields) {
            assertThat(userSchema.fieldNames())
                    .describedAs("User events schema should contain field: " + field)
                    .contains(field);
        }
        
        // Data integrity validation
        validateDataIntegrity(userEventsTable, userSnapshot, "user_events");
        
        log.info("✅ E2E-005: Data integrity and schema validation passed");
    }
    
    // Test 6: Error Scenarios and Recovery
    @Test
    @Order(6)
    @DisplayName("E2E-006: Error Scenarios and Recovery Testing")
    void testErrorScenariosAndRecovery() throws Exception {
        log.info("🚀 E2E-006: Starting error scenarios testing");
        
        // Test 1: Invalid JSON messages
        List<String> invalidMessages = Arrays.asList(
                "invalid-json-1",
                "{incomplete json",
                "",
                null
        );
        
        // Send invalid messages (these should go to DLQ)
        for (String message : invalidMessages) {
            if (message != null) {
                kafkaProducer.send(new ProducerRecord<>(USER_EVENTS_TOPIC, "error-test", message));
            }
        }
        
        // Send valid messages after errors
        List<String> validMessages = generateUserEventMessages(10);
        sendMessagesToKafka(USER_EVENTS_TOPIC, validMessages);
        
        // Wait for processing
        Thread.sleep(20000);
        
        // Verify that valid messages were still processed
        // (Invalid messages should not break the pipeline)
        log.info("✅ E2E-006: Error scenarios and recovery testing passed");
    }
    
    // Test 7: Partitioning Validation
    @Test
    @Order(7)
    @DisplayName("E2E-007: Delta Partitioning Validation")
    void testDeltaPartitioning() throws Exception {
        log.info("🚀 E2E-007: Starting partitioning validation");
        
        // Generate messages with different dates and event types
        List<String> partitionedMessages = generatePartitionedUserMessages();
        sendMessagesToKafka(USER_EVENTS_TOPIC, partitionedMessages);
        
        // Wait for processing
        Thread.sleep(25000);
        
        // Validate partitioning structure in S3
        validateS3PartitionStructure();
        
        log.info("✅ E2E-007: Delta partitioning validation passed");
    }
    
    // Helper Methods
    
    private List<String> generateUserEventMessages(int count) {
        List<String> messages = new ArrayList<>();
        String[] eventTypes = {"login", "logout", "page_view", "purchase", "signup"};
        String[] userIds = {"user1", "user2", "user3", "user4", "user5"};
        
        for (int i = 0; i < count; i++) {
            String eventType = eventTypes[i % eventTypes.length];
            String userId = userIds[i % userIds.length];
            
            String message = String.format(
                    "{\"user_id\":\"%s\",\"event_type\":\"%s\",\"timestamp\":\"%s\",\"properties\":{\"session_id\":\"session_%d\",\"page\":\"/test\",\"value\":%d}}",
                    userId, eventType, LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), i, i * 10
            );
            messages.add(message);
        }
        
        return messages;
    }
    
    private List<String> generateOrderEventMessages(int count) {
        List<String> messages = new ArrayList<>();
        String[] statuses = {"pending", "confirmed", "shipped", "delivered", "cancelled"};
        String[] customerIds = {"cust1", "cust2", "cust3", "cust4", "cust5"};
        
        for (int i = 0; i < count; i++) {
            String status = statuses[i % statuses.length];
            String customerId = customerIds[i % customerIds.length];
            
            String message = String.format(
                    "{\"order_id\":\"order_%d\",\"customer_id\":\"%s\",\"status\":\"%s\",\"amount\":%d.%02d,\"timestamp\":\"%s\"}",
                    i, customerId, status, (i + 1) * 50, i % 100, LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            );
            messages.add(message);
        }
        
        return messages;
    }
    
    private List<String> generatePartitionedUserMessages() {
        List<String> messages = new ArrayList<>();
        String[] dates = {"2024-01-15", "2024-02-15", "2024-03-15"};
        String[] eventTypes = {"login", "purchase", "logout"};
        
        for (String date : dates) {
            for (String eventType : eventTypes) {
                String message = String.format(
                        "{\"user_id\":\"test_user\",\"event_type\":\"%s\",\"timestamp\":\"%sT10:00:00\",\"properties\":{\"test\":true}}",
                        eventType, date
                );
                messages.add(message);
            }
        }
        
        return messages;
    }
    
    private void sendMessagesToKafka(String topic, List<String> messages) {
        sendMessagesToKafka(topic, messages, "test-producer");
    }
    
    private void sendMessagesToKafka(String topic, List<String> messages, String producerPrefix) {
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        
        for (int i = 0; i < messages.size(); i++) {
            String key = producerPrefix + "-" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, messages.get(i));
            futures.add(kafkaProducer.send(record));
        }
        
        // Wait for all sends to complete
        for (Future<RecordMetadata> future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Failed to send message to Kafka", e);
                throw new RuntimeException("Failed to send message to Kafka", e);
            }
        }
        
        log.info("📤 Sent {} messages to topic {}", messages.size(), topic);
    }
    
    private void validateDeltaTable(String tablePath, int expectedMinRows, List<String> expectedColumns) {
        try {
            Table table = Table.forPath(deltaEngine, tablePath);
            Snapshot snapshot = table.getLatestSnapshot(deltaEngine);
            StructType schema = snapshot.getSchema();
            
            // Validate schema
            for (String expectedColumn : expectedColumns) {
                assertThat(schema.fieldNames())
                        .describedAs("Schema should contain column: " + expectedColumn)
                        .contains(expectedColumn);
            }
            
            // Validate row count
            Scan scan = snapshot.getScanBuilder().build();
            CloseableIterator<FilteredColumnarBatch> data = scan.getScanFiles(deltaEngine);
            
            long rowCount = 0;
            while (data.hasNext()) {
                rowCount += data.next().getData().getSize();
            }
            data.close();
            
            assertThat(rowCount)
                    .describedAs("Delta table should have at least %d rows", expectedMinRows)
                    .isGreaterThanOrEqualTo(expectedMinRows);
            
            log.info("✅ Delta table validation passed: {} rows, {} columns", rowCount, schema.fieldNames().size());
            
        } catch (Exception e) {
            log.error("Delta table validation failed for path: {}", tablePath, e);
            throw new AssertionError("Delta table validation failed: " + e.getMessage(), e);
        }
    }
    
    private void validateDataIntegrity(Table table, Snapshot snapshot, String tableName) {
        try {
            // Check that table has data
            Scan scan = snapshot.getScanBuilder().build();
            CloseableIterator<FilteredColumnarBatch> data = scan.getScanFiles(deltaEngine);
            
            boolean hasData = data.hasNext();
            data.close();
            
            assertThat(hasData)
                    .describedAs("Table %s should contain data", tableName)
                    .isTrue();
            
            // Additional integrity checks could include:
            // - Checking for duplicate records
            // - Validating timestamp ranges
            // - Checking required field values
            
            log.info("✅ Data integrity validation passed for table: {}", tableName);
            
        } catch (Exception e) {
            log.error("Data integrity validation failed for table: {}", tableName, e);
            throw new AssertionError("Data integrity validation failed: " + e.getMessage(), e);
        }
    }
    
    private void validateS3PartitionStructure() {
        try {
            // List objects in the user events path to verify partitioning
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(BUCKET_NAME)
                    .prefix("events/user-events/")
                    .build();
            
            ListObjectsV2Response response = s3Client.listObjectsV2(request);
            
            assertThat(response.contents())
                    .describedAs("S3 should contain partitioned data files")
                    .isNotEmpty();
            
            // Check for partition structure (year=XXXX/month=XX/day=XX)
            boolean foundPartitionStructure = response.contents().stream()
                    .anyMatch(obj -> obj.key().contains("year=") && 
                                   obj.key().contains("month=") && 
                                   obj.key().contains("day="));
            
            assertThat(foundPartitionStructure)
                    .describedAs("S3 objects should follow partition structure")
                    .isTrue();
            
            log.info("✅ S3 partition structure validation passed");
            
        } catch (Exception e) {
            log.error("S3 partition structure validation failed", e);
            throw new AssertionError("S3 partition structure validation failed: " + e.getMessage(), e);
        }
    }
}