package com.company.kafkaconnector.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.*;

/**
 * S3 Delta Lake Sink Connector implementing proper Kafka Connect API
 * Following Single Responsibility Principle - only handles connector lifecycle
 */
@Slf4j
public class S3DeltaSinkConnector extends SinkConnector {
    
    public static final String VERSION = "1.0.0";
    
    // Configuration keys following naming conventions
    public static final String S3_BUCKET_CONFIG = "s3.bucket.name";
    public static final String S3_REGION_CONFIG = "s3.region";
    public static final String S3_ACCESS_KEY_CONFIG = "s3.access.key.id";
    public static final String S3_SECRET_KEY_CONFIG = "s3.secret.access.key";
    public static final String S3_ENDPOINT_CONFIG = "s3.endpoint.url";
    public static final String TOPICS_DIR_CONFIG = "topics.dir";
    public static final String FLUSH_SIZE_CONFIG = "flush.size";
    public static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";
    public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";
    public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final String MAX_RETRIES_CONFIG = "max.retries";
    
    private Map<String, String> configProps;
    
    @Override
    public String version() {
        return VERSION;
    }
    
    @Override
    public void start(Map<String, String> props) {
        log.info("Starting S3 Delta Sink Connector version {}", version());
        
        // Validate configuration
        validateConfig(props);
        this.configProps = new HashMap<>(props);
        
        log.info("S3 Delta Sink Connector started with {} configuration properties", props.size());
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        return S3DeltaSinkTask.class;
    }
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} task configurations", maxTasks);
        
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put("task.id", String.valueOf(i));
            taskConfigs.add(taskConfig);
        }
        
        return taskConfigs;
    }
    
    @Override
    public void stop() {
        log.info("Stopping S3 Delta Sink Connector");
        // Cleanup resources if needed
    }
    
    @Override
    public ConfigDef config() {
        return createConfigDef();
    }
    
    /**
     * Factory method for creating configuration definition
     * Following Open/Closed Principle - extensible without modification
     */
    private ConfigDef createConfigDef() {
        return new ConfigDef()
            // S3 Configuration
            .define(S3_BUCKET_CONFIG,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                "S3 bucket name where data will be stored")
                
            .define(S3_REGION_CONFIG,
                Type.STRING,
                "us-east-1",
                Importance.MEDIUM,
                "AWS region for S3 bucket")
                
            .define(S3_ACCESS_KEY_CONFIG,
                Type.PASSWORD,
                null,
                Importance.HIGH,
                "AWS access key ID")
                
            .define(S3_SECRET_KEY_CONFIG,
                Type.PASSWORD,
                null,
                Importance.HIGH,
                "AWS secret access key")
                
            .define(S3_ENDPOINT_CONFIG,
                Type.STRING,
                null,
                Importance.LOW,
                "Custom S3 endpoint URL (for MinIO or other S3-compatible storage)")
                
            // Topic and partitioning configuration
            .define(TOPICS_DIR_CONFIG,
                Type.STRING,
                "topics",
                Importance.MEDIUM,
                "Root directory in S3 bucket for topic data")
                
            .define(PARTITIONER_CLASS_CONFIG,
                Type.CLASS,
                "com.company.kafkaconnector.partitioner.DefaultPartitioner",
                Importance.MEDIUM,
                "Partitioner class for organizing data in S3")
                
            // Flush and rotation settings
            .define(FLUSH_SIZE_CONFIG,
                Type.INT,
                1000,
                ConfigDef.Range.atLeast(1),
                Importance.MEDIUM,
                "Number of records to write before flushing to S3")
                
            .define(ROTATE_INTERVAL_MS_CONFIG,
                Type.LONG,
                60000L,
                ConfigDef.Range.atLeast(1000L),
                Importance.MEDIUM,
                "Time interval in milliseconds for rotating files")
                
            // Error handling and retry configuration
            .define(MAX_RETRIES_CONFIG,
                Type.INT,
                3,
                ConfigDef.Range.atLeast(0),
                Importance.MEDIUM,
                "Maximum number of retries for failed operations")
                
            .define(RETRY_BACKOFF_MS_CONFIG,
                Type.LONG,
                1000L,
                ConfigDef.Range.atLeast(0L),
                Importance.LOW,
                "Backoff time in milliseconds between retries")
                
            // Schema compatibility
            .define(SCHEMA_COMPATIBILITY_CONFIG,
                Type.STRING,
                "BACKWARD",
                ConfigDef.ValidString.in("BACKWARD", "FORWARD", "FULL", "NONE"),
                Importance.MEDIUM,
                "Schema evolution compatibility type");
    }
    
    /**
     * Validates connector configuration
     * Following Single Responsibility Principle
     */
    private void validateConfig(Map<String, String> props) {
        if (StringUtils.isBlank(props.get(S3_BUCKET_CONFIG))) {
            throw new ConfigException("S3 bucket name is required");
        }
        
        // Validate flush size
        String flushSizeStr = props.get(FLUSH_SIZE_CONFIG);
        if (flushSizeStr != null) {
            try {
                int flushSize = Integer.parseInt(flushSizeStr);
                if (flushSize < 1) {
                    throw new ConfigException("Flush size must be at least 1");
                }
            } catch (NumberFormatException e) {
                throw new ConfigException("Invalid flush size: " + flushSizeStr);
            }
        }
        
        log.debug("Configuration validation completed successfully");
    }
}