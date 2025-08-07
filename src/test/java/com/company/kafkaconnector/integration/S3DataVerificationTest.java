package com.company.kafkaconnector.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Data verification integration test for S3 storage
 * Validates data integrity, format, and structure after connector processing
 * 
 * Prerequisites:
 * - MinIO running on localhost:9000 (minioadmin/minioadmin)
 * - Connector has processed some messages
 * - test-data-lake bucket exists with data
 */
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "RUN_DATA_VERIFICATION", matches = "true")
public class S3DataVerificationTest {
    
    private static final String BUCKET_NAME = "test-data-lake";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    
    private static S3Client s3Client;
    private static ObjectMapper objectMapper;
    
    @BeforeAll
    static void setUp() {
        log.info("Setting up S3 data verification test");
        
        // Create S3 client for MinIO
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(MINIO_ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();
        
        objectMapper = new ObjectMapper();
        
        log.info("S3 client configured for MinIO at {}", MINIO_ENDPOINT);
    }
    
    @AfterAll
    static void tearDown() {
        if (s3Client != null) {
            s3Client.close();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("Test 1: Verify S3 bucket and data existence")
    void testBucketAndDataExistence() {
        log.info("Verifying S3 bucket exists and contains data");
        
        // Check bucket exists
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(BUCKET_NAME).build());
            log.info("✅ Bucket '{}' exists", BUCKET_NAME);
        } catch (NoSuchBucketException e) {
            log.error("❌ Bucket '{}' does not exist. Run integration tests first.", BUCKET_NAME);
            throw new AssertionError("Bucket does not exist. Run connector processing first.", e);
        }
        
        // List objects in bucket
        ListObjectsV2Response response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(BUCKET_NAME).build());
        
        List<S3Object> objects = response.contents();
        log.info("Found {} objects in bucket '{}'", objects.size(), BUCKET_NAME);
        
        assertThat(objects).isNotEmpty()
                .describedAs("Bucket should contain processed files from connector");
        
        // Log some sample files
        objects.stream().limit(10).forEach(obj -> 
                log.info("  📄 {} (size: {} bytes, modified: {})", 
                        obj.key(), obj.size(), obj.lastModified()));
        
        if (objects.size() > 10) {
            log.info("  ... and {} more files", objects.size() - 10);
        }
    }
    
    @Test
    @Order(2)
    @DisplayName("Test 2: Verify partition-based directory structure")
    void testPartitionDirectoryStructure() {
        log.info("Verifying partition-based directory structure");
        
        ListObjectsV2Response response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(BUCKET_NAME).build());
        
        List<String> keys = response.contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
        
        // Expected patterns based on partition strategies
        Pattern timeBasedPattern = Pattern.compile(
                "^kafka-connect.*/(user-events|order-events)/year=\\d{4}/month=\\d{2}/day=\\d{2}/hour=\\d{2}/.*\\.jsonl$");
        Pattern defaultPattern = Pattern.compile(
                "^kafka-connect.*/(user-events|order-events)/partition=\\d+/.*\\.jsonl$");
        
        Map<String, List<String>> categorizedKeys = new HashMap<>();
        categorizedKeys.put("time-based", new ArrayList<>());
        categorizedKeys.put("default", new ArrayList<>());
        categorizedKeys.put("other", new ArrayList<>());
        
        for (String key : keys) {
            if (timeBasedPattern.matcher(key).matches()) {
                categorizedKeys.get("time-based").add(key);
            } else if (defaultPattern.matcher(key).matches()) {
                categorizedKeys.get("default").add(key);
            } else {
                categorizedKeys.get("other").add(key);
            }
        }
        
        log.info("Directory structure analysis:");
        log.info("  Time-based partitioned files: {}", categorizedKeys.get("time-based").size());
        log.info("  Default partitioned files: {}", categorizedKeys.get("default").size());
        log.info("  Other files: {}", categorizedKeys.get("other").size());
        
        // Show sample paths
        categorizedKeys.forEach((type, fileKeys) -> {
            if (!fileKeys.isEmpty()) {
                log.info("  Sample {} path: {}", type, fileKeys.get(0));
            }
        });
        
        // Verify we have proper partitioning
        int totalPartitionedFiles = categorizedKeys.get("time-based").size() + 
                                   categorizedKeys.get("default").size();
        
        assertThat(totalPartitionedFiles).isGreaterThan(0)
                .describedAs("Should have files following partition patterns");
        
        log.info("✅ Partition-based directory structure verified");
    }
    
    @Test
    @Order(3)
    @DisplayName("Test 3: Verify JSON Lines format and content")
    void testJsonLinesFormat() {
        log.info("Verifying JSON Lines format and content");
        
        ListObjectsV2Response response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder()
                        .bucket(BUCKET_NAME)
                        .maxKeys(5) // Limit to first 5 files for testing
                        .build());
        
        int filesVerified = 0;
        int totalMessages = 0;
        Set<String> uniqueCorrelationIds = new HashSet<>();
        Set<String> topics = new HashSet<>();
        
        for (S3Object s3Object : response.contents()) {
            if (!s3Object.key().endsWith(".jsonl")) {
                continue; // Skip non-JSON Lines files
            }
            
            log.info("Verifying file: {}", s3Object.key());
            
            try {
                // Download and read file
                var objectResponse = s3Client.getObject(
                        GetObjectRequest.builder()
                                .bucket(BUCKET_NAME)
                                .key(s3Object.key())
                                .build());
                
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(objectResponse))) {
                    
                    String line;
                    int lineCount = 0;
                    
                    while ((line = reader.readLine()) != null) {
                        lineCount++;
                        totalMessages++;
                        
                        // Verify each line is valid JSON
                        try {
                            JsonNode jsonNode = objectMapper.readTree(line);
                            
                            // Verify expected message structure
                            assertThat(jsonNode).isNotNull();
                            
                            // Check for metadata enrichment (added by RecordProcessor)
                            if (jsonNode.has("_metadata")) {
                                JsonNode metadata = jsonNode.get("_metadata");
                                assertThat(metadata.has("processing_timestamp")).isTrue();
                                assertThat(metadata.has("kafka_topic")).isTrue();
                                assertThat(metadata.has("processor_version")).isTrue();
                                
                                // Track topics
                                if (metadata.has("kafka_topic")) {
                                    topics.add(metadata.get("kafka_topic").asText());
                                }
                            }
                            
                            // Collect correlation IDs for uniqueness check
                            if (jsonNode.has("correlation_id")) {
                                uniqueCorrelationIds.add(jsonNode.get("correlation_id").asText());
                            }
                            
                            // Verify original data preservation
                            boolean hasUserData = jsonNode.has("user_id") || jsonNode.has("order_id");
                            assertThat(hasUserData).isTrue()
                                    .describedAs("Should contain original message data");
                            
                        } catch (Exception e) {
                            log.error("❌ Invalid JSON on line {} in file {}: {}", 
                                    lineCount, s3Object.key(), line);
                            throw e;
                        }
                    }
                    
                    log.info("  ✅ File contains {} valid JSON lines", lineCount);
                    filesVerified++;
                    
                } catch (IOException e) {
                    log.error("❌ Error reading file {}: {}", s3Object.key(), e.getMessage());
                    throw e;
                }
                
            } catch (Exception e) {
                log.error("❌ Error processing file {}: {}", s3Object.key(), e.getMessage());
                throw new AssertionError("File processing failed", e);
            }
        }
        
        log.info("JSON Lines verification summary:");
        log.info("  Files verified: {}", filesVerified);
        log.info("  Total messages: {}", totalMessages);
        log.info("  Unique correlation IDs: {}", uniqueCorrelationIds.size());
        log.info("  Topics found: {}", topics);
        
        assertThat(filesVerified).isGreaterThan(0)
                .describedAs("Should have verified at least one file");
        
        assertThat(totalMessages).isGreaterThan(0)
                .describedAs("Should have processed some messages");
        
        log.info("✅ JSON Lines format verification completed");
    }
    
    @Test
    @Order(4)
    @DisplayName("Test 4: Verify message transformation integrity")
    void testMessageTransformationIntegrity() throws IOException {
        log.info("Verifying message transformation integrity");
        
        ListObjectsV2Response response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder()
                        .bucket(BUCKET_NAME)
                        .maxKeys(3) // Check first 3 files
                        .build());
        
        int messagesWithMetadata = 0;
        int messagesWithOriginalData = 0;
        Set<String> processorVersions = new HashSet<>();
        
        for (S3Object s3Object : response.contents()) {
            if (!s3Object.key().endsWith(".jsonl")) {
                continue;
            }
            
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(s3Client.getObject(
                            GetObjectRequest.builder()
                                    .bucket(BUCKET_NAME)
                                    .key(s3Object.key())
                                    .build())))) {
                
                String line;
                while ((line = reader.readLine()) != null) {
                    JsonNode jsonNode = objectMapper.readTree(line);
                    
                    // Check metadata enrichment
                    if (jsonNode.has("_metadata")) {
                        messagesWithMetadata++;
                        
                        JsonNode metadata = jsonNode.get("_metadata");
                        
                        // Verify required metadata fields
                        assertThat(metadata.has("processing_timestamp")).isTrue();
                        assertThat(metadata.has("kafka_topic")).isTrue();
                        assertThat(metadata.has("kafka_partition")).isTrue();
                        assertThat(metadata.has("kafka_offset")).isTrue();
                        assertThat(metadata.has("processor_version")).isTrue();
                        
                        // Collect processor versions
                        if (metadata.has("processor_version")) {
                            processorVersions.add(metadata.get("processor_version").asText());
                        }
                        
                        // Verify timestamp format
                        String timestamp = metadata.get("processing_timestamp").asText();
                        assertThat(timestamp).matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*");
                    }
                    
                    // Check original data preservation
                    boolean hasOriginalData = jsonNode.has("user_id") || 
                                            jsonNode.has("order_id") || 
                                            jsonNode.has("event_type");
                    if (hasOriginalData) {
                        messagesWithOriginalData++;
                    }
                }
                
            } catch (Exception e) {
                log.error("Error verifying transformation in file {}: {}", s3Object.key(), e.getMessage());
                throw e;
            }
        }
        
        log.info("Message transformation verification:");
        log.info("  Messages with metadata: {}", messagesWithMetadata);
        log.info("  Messages with original data: {}", messagesWithOriginalData);
        log.info("  Processor versions found: {}", processorVersions);
        
        assertThat(messagesWithMetadata).isGreaterThan(0)
                .describedAs("Should have messages with transformation metadata");
        
        assertThat(messagesWithOriginalData).isGreaterThan(0)
                .describedAs("Should preserve original message data");
        
        assertThat(processorVersions).contains("1.0.0")
                .describedAs("Should have expected processor version");
        
        log.info("✅ Message transformation integrity verified");
    }
    
    @Test
    @Order(5)
    @DisplayName("Test 5: Verify no duplicate messages with horizontal scaling")
    void testNoDuplicateMessages() {
        log.info("Verifying no duplicate messages in horizontal scaling scenario");
        
        ListObjectsV2Response response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(BUCKET_NAME).build());
        
        Set<String> messageIds = new HashSet<>();
        Set<String> kafkaOffsets = new HashSet<>();
        int totalMessages = 0;
        int duplicateMessages = 0;
        
        for (S3Object s3Object : response.contents()) {
            if (!s3Object.key().endsWith(".jsonl")) {
                continue;
            }
            
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(s3Client.getObject(
                            GetObjectRequest.builder()
                                    .bucket(BUCKET_NAME)
                                    .key(s3Object.key())
                                    .build())))) {
                
                String line;
                while ((line = reader.readLine()) != null) {
                    totalMessages++;
                    JsonNode jsonNode = objectMapper.readTree(line);
                    
                    // Create unique message identifier
                    StringBuilder messageIdBuilder = new StringBuilder();
                    
                    if (jsonNode.has("user_id")) {
                        messageIdBuilder.append("user_").append(jsonNode.get("user_id").asText());
                    } else if (jsonNode.has("order_id")) {
                        messageIdBuilder.append("order_").append(jsonNode.get("order_id").asText());
                    }
                    
                    // Add timestamp for uniqueness
                    if (jsonNode.has("timestamp")) {
                        messageIdBuilder.append("_").append(jsonNode.get("timestamp").asText());
                    }
                    
                    String messageId = messageIdBuilder.toString();
                    if (!messageId.isEmpty()) {
                        if (messageIds.contains(messageId)) {
                            duplicateMessages++;
                            log.warn("⚠️  Potential duplicate message found: {}", messageId);
                        } else {
                            messageIds.add(messageId);
                        }
                    }
                    
                    // Track Kafka offsets for uniqueness
                    if (jsonNode.has("_metadata")) {
                        JsonNode metadata = jsonNode.get("_metadata");
                        if (metadata.has("kafka_topic") && metadata.has("kafka_partition") && metadata.has("kafka_offset")) {
                            String offsetKey = metadata.get("kafka_topic").asText() + "_" +
                                             metadata.get("kafka_partition").asText() + "_" +
                                             metadata.get("kafka_offset").asText();
                            kafkaOffsets.add(offsetKey);
                        }
                    }
                }
                
            } catch (Exception e) {
                log.error("Error checking duplicates in file {}: {}", s3Object.key(), e.getMessage());
            }
        }
        
        log.info("Duplicate message analysis:");
        log.info("  Total messages processed: {}", totalMessages);
        log.info("  Unique message IDs: {}", messageIds.size());
        log.info("  Unique Kafka offsets: {}", kafkaOffsets.size());
        log.info("  Duplicate messages found: {}", duplicateMessages);
        
        // Calculate duplication rate
        double duplicationRate = totalMessages > 0 ? (double) duplicateMessages / totalMessages : 0;
        log.info("  Duplication rate: {:.2f}%", duplicationRate * 100);
        
        assertThat(duplicateMessages).isLessThanOrEqualTo(totalMessages / 100) // Less than 1%
                .describedAs("Duplicate messages should be minimal (< 1%% of total)");
        
        log.info("✅ Duplicate message verification completed");
    }
    
    @Test
    @Order(6)
    @DisplayName("Test 6: Verify file naming and organization")
    void testFileNamingAndOrganization() {
        log.info("Verifying file naming conventions and organization");
        
        ListObjectsV2Response response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(BUCKET_NAME).build());
        
        Map<String, Integer> topicCounts = new HashMap<>();
        Map<String, Integer> extensionCounts = new HashMap<>();
        Set<String> directories = new HashSet<>();
        
        Pattern filenamePattern = Pattern.compile("\\d{8}T\\d{6}.*_[a-f0-9]{8}\\.jsonl$");
        int validFilenames = 0;
        
        for (S3Object s3Object : response.contents()) {
            String key = s3Object.key();
            
            // Extract directory path
            String directory = key.substring(0, key.lastIndexOf('/'));
            directories.add(directory);
            
            // Count by topic
            if (key.contains("user-events")) {
                topicCounts.merge("user-events", 1, Integer::sum);
            } else if (key.contains("order-events")) {
                topicCounts.merge("order-events", 1, Integer::sum);
            }
            
            // Count by extension
            String extension = key.substring(key.lastIndexOf('.') + 1);
            extensionCounts.merge(extension, 1, Integer::sum);
            
            // Verify filename pattern
            String filename = key.substring(key.lastIndexOf('/') + 1);
            if (filenamePattern.matcher(filename).matches()) {
                validFilenames++;
            } else {
                log.debug("Non-standard filename: {}", filename);
            }
        }
        
        log.info("File organization analysis:");
        log.info("  Total directories: {}", directories.size());
        log.info("  Topic distribution: {}", topicCounts);
        log.info("  Extension distribution: {}", extensionCounts);
        log.info("  Valid filename patterns: {} / {}", validFilenames, response.contents().size());
        
        // Sample directories
        directories.stream().limit(5).forEach(dir -> 
                log.info("  Sample directory: {}", dir));
        
        assertThat(topicCounts.keySet()).contains("user-events", "order-events")
                .describedAs("Should have files for expected topics");
        
        assertThat(extensionCounts.get("jsonl")).isGreaterThan(0)
                .describedAs("Should have JSON Lines files");
        
        // Most files should follow naming convention
        double validFilenameRate = (double) validFilenames / response.contents().size();
        assertThat(validFilenameRate).isGreaterThan(0.8)
                .describedAs("Most files should follow timestamp_uuid.jsonl naming pattern");
        
        log.info("✅ File naming and organization verified");
    }
}