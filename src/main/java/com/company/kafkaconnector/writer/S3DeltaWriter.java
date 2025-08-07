package com.company.kafkaconnector.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * S3 Delta writer implementation
 * Following Builder Pattern and Single Responsibility Principle
 */
@Slf4j
@Builder
public class S3DeltaWriter implements S3Writer {
    
    private final Map<String, String> config;
    private final String partitionKey;
    private final String bucketName;
    private final String region;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String endpointUrl;
    private final String topicsDir;
    
    private S3Client s3Client;
    private ObjectMapper objectMapper;
    private String currentFilePath;
    
    // Statistics tracking
    private final AtomicLong recordsWritten = new AtomicLong(0);
    private final AtomicLong bytesWritten = new AtomicLong(0);
    private final AtomicLong filesCreated = new AtomicLong(0);
    private final AtomicLong writeErrors = new AtomicLong(0);
    private final Instant createdTime = Instant.now();
    private volatile Instant lastWriteTime;
    
    /**
     * Initialize the writer
     * Following lazy initialization pattern
     */
    private void initialize() {
        if (s3Client == null) {
            synchronized (this) {
                if (s3Client == null) {
                    s3Client = createS3Client();
                    objectMapper = new ObjectMapper();
                    log.info("S3DeltaWriter initialized for partition: {}", partitionKey);
                }
            }
        }
    }
    
    @Override
    public void write(List<SinkRecord> records) throws Exception {
        initialize();
        
        if (records == null || records.isEmpty()) {
            return;
        }
        
        try {
            // Generate unique file name for this batch
            String fileName = generateFileName();
            currentFilePath = String.format("%s/%s/%s", topicsDir, partitionKey, fileName);
            
            // Convert records to JSON lines format
            StringBuilder jsonLines = new StringBuilder();
            for (SinkRecord record : records) {
                String jsonLine = objectMapper.writeValueAsString(record.value());
                jsonLines.append(jsonLine).append("\n");
            }
            
            // Write to S3
            byte[] data = jsonLines.toString().getBytes();
            PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(currentFilePath)
                .contentLength((long) data.length)
                .contentType("application/x-ndjson")
                .build();
                
            s3Client.putObject(putRequest, RequestBody.fromBytes(data));
            
            // Update statistics
            recordsWritten.addAndGet(records.size());
            bytesWritten.addAndGet(data.length);
            filesCreated.incrementAndGet();
            lastWriteTime = Instant.now();
            
            log.debug("Successfully wrote {} records ({} bytes) to {}", 
                     records.size(), data.length, currentFilePath);
            
        } catch (Exception e) {
            writeErrors.incrementAndGet();
            log.error("Failed to write {} records to S3: {}", records.size(), e.getMessage(), e);
            throw e;
        }
    }
    
    @Override
    public void flush() throws Exception {
        // For file-based writing, flush is implicit with each write
        log.debug("Flush completed for partition: {}", partitionKey);
    }
    
    @Override
    public String getCurrentFilePath() {
        return currentFilePath;
    }
    
    @Override
    public WriterStats getStats() {
        return WriterStats.builder()
            .recordsWritten(recordsWritten.get())
            .bytesWritten(bytesWritten.get())
            .filesCreated(filesCreated.get())
            .writeErrors(writeErrors.get())
            .lastWriteTime(lastWriteTime)
            .createdTime(createdTime)
            .currentFilePath(currentFilePath)
            .build();
    }
    
    @Override
    public void close() throws Exception {
        if (s3Client != null) {
            s3Client.close();
            log.info("S3DeltaWriter closed for partition: {}", partitionKey);
        }
    }
    
    /**
     * Create S3 client based on configuration
     * Following Builder Pattern
     */
    private S3Client createS3Client() {
        S3ClientBuilder builder = S3Client.builder();
        
        // Set region
        if (StringUtils.isNotBlank(region)) {
            builder.region(Region.of(region));
        }
        
        // Set credentials if provided
        if (StringUtils.isNotBlank(accessKeyId) && StringUtils.isNotBlank(secretAccessKey)) {
            AwsCredentials credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
            builder.credentialsProvider(StaticCredentialsProvider.create(credentials));
        }
        
        // Set custom endpoint (for MinIO)
        if (StringUtils.isNotBlank(endpointUrl)) {
            builder.endpointOverride(URI.create(endpointUrl));
            builder.forcePathStyle(true); // Required for MinIO
        }
        
        return builder.build();
    }
    
    /**
     * Generate unique file name for the batch
     * Following time-based naming convention
     */
    private String generateFileName() {
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
            .replaceAll("[:\\-.]", "");
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        return String.format("%s_%s.jsonl", timestamp, uuid);
    }
}