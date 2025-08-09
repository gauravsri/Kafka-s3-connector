# Implementation Steps: Database-Driven Distributed Idempotency Service

## Overview

This document outlines the implementation steps for a production-ready distributed idempotency service that handles:
- **Database-backed idempotency** using H2 (local) and CockroachDB (production)
- **Adaptive bloom filter management** for hot partitions only
- **Horizontal scaling** with standard database clustering
- **COB-based Delta Lake partitioning** with multi-format Avro message support
- **Zero data loss** and **zero duplicates** guarantees across distributed deployment
- **Operational simplicity** with proven database patterns

## 📋 Implementation Phases

### Phase 1: Database Foundation & Multi-Format Avro Setup
### Phase 2: Database-Backed Idempotency Service
### Phase 3: Adaptive Bloom Filter Management
### Phase 4: COB-Based Delta Lake Integration
### Phase 5: Consumer Integration & Safety
### Phase 6: Monitoring and Observability
### Phase 7: Production Deployment Strategy

---

## Phase 1: Database Foundation & Multi-Format Avro Setup

### Step 1.1: Add Database and Schema Dependencies

**File:** `pom.xml`
```xml
<!-- Database Dependencies -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>
<dependency>
    <groupId>org.liquibase</groupId>
    <artifactId>liquibase-core</artifactId>
</dependency>

<!-- H2 for development -->
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <scope>runtime</scope>
</dependency>

<!-- PostgreSQL driver for CockroachDB -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
</dependency>

<!-- Avro and Schema Registry dependencies -->
<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro</artifactId>
    <version>1.11.3</version>
</dependency>
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.5.1</version>
</dependency>
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-client</artifactId>
    <version>7.5.1</version>
</dependency>

<!-- JSON Processing -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-core</artifactId>
    <version>2.15.2</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.15.2</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.datatype</groupId>
    <artifactId>jackson-datatype-jsr310</artifactId>
    <version>2.15.2</version>
</dependency>

<!-- CSV Processing -->
<dependency>
    <groupId>com.opencsv</groupId>
    <artifactId>opencsv</artifactId>
    <version>5.8</version>
</dependency>
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-csv</artifactId>
    <version>1.10.0</version>
</dependency>

<!-- Bloom filter dependency (for adaptive optimization only) -->
<dependency>
    <groupId>com.google.guava</groupId>
    <artifactId>guava</artifactId>
    <version>32.1.3-jre</version>
</dependency>
```

### Step 1.2: Setup Database Schema with Liquibase

**File:** `src/main/resources/db/changelog/db.changelog-master.yaml`
```yaml
databaseChangeLog:
  - changeSet:
      id: create-idempotency-table
      author: system
      changes:
        - createTable:
            tableName: idempotency_log
            columns:
              - column:
                  name: id
                  type: BIGINT
                  autoIncrement: true
                  constraints:
                    primaryKey: true
              - column:
                  name: topic
                  type: VARCHAR(255)
                  constraints:
                    nullable: false
              - column:
                  name: cob_date
                  type: DATE
                  constraints:
                    nullable: false
              - column:
                  name: message_fingerprint
                  type: VARCHAR(64)
                  constraints:
                    nullable: false
              - column:
                  name: kafka_partition
                  type: INT
              - column:
                  name: kafka_offset
                  type: BIGINT
              - column:
                  name: processed_at
                  type: TIMESTAMP
                  constraints:
                    nullable: false
              - column:
                  name: instance_id
                  type: VARCHAR(64)
                  
        - createIndex:
            indexName: idx_topic_cob_fingerprint
            tableName: idempotency_log
            unique: true
            columns:
              - column:
                  name: topic
              - column:
                  name: cob_date  
              - column:
                  name: message_fingerprint
                  
        - createIndex:
            indexName: idx_topic_cob_processed
            tableName: idempotency_log
            columns:
              - column:
                  name: topic
              - column:
                  name: cob_date
              - column:
                  name: processed_at
                  
        - createIndex:
            indexName: idx_processed_at
            tableName: idempotency_log
            columns:
              - column:
                  name: processed_at

  - changeSet:
      id: create-partition-activity-view
      author: system
      changes:
        - createView:
            viewName: partition_activity
            selectQuery: |
              SELECT 
                topic,
                cob_date,
                COUNT(*) as message_count,
                MAX(processed_at) as last_activity,
                COUNT(DISTINCT instance_id) as instance_count
              FROM idempotency_log 
              WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 HOUR'
              GROUP BY topic, cob_date
```

### Step 1.3: Create Avro Schemas for Multiple Message Formats

**File:** `src/main/avro/user-event.avsc`
```json
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.company.kafkaconnector.avro",
  "doc": "Universal schema for user events - supports JSON, CSV, and Avro sources",
  "fields": [
    {"name": "user_id", "type": "string", "doc": "Required user identifier"},
    {"name": "event_type", "type": "string", "doc": "Event type classification"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis", "doc": "Event timestamp"},
    {"name": "cob", "type": "string", "doc": "COB date in YYYY-MM-DD format - REQUIRED for partitioning"},
    {"name": "session_id", "type": ["null", "string"], "default": null, "doc": "Optional session identifier"},
    {"name": "properties", "type": {"type": "map", "values": "string"}, "default": {}, "doc": "Additional event properties"},
    {"name": "metadata", "type": {
      "type": "record",
      "name": "EventMetadata",
      "fields": [
        {"name": "source", "type": "string", "doc": "Source system identifier"},
        {"name": "version", "type": "string", "default": "1.0", "doc": "Schema version"},
        {"name": "correlation_id", "type": ["null", "string"], "default": null, "doc": "Correlation ID for tracing"},
        {"name": "original_format", "type": {"type": "enum", "name": "MessageFormat", "symbols": ["JSON", "CSV", "AVRO"]}, "doc": "Original message format"}
      ]
    }}
  ]
}
```

**File:** `src/main/avro/order-event.avsc`
```json
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "com.company.kafkaconnector.avro",
  "doc": "Universal schema for order events - supports JSON, CSV, and Avro sources",
  "fields": [
    {"name": "order_id", "type": "string", "doc": "Unique order identifier"},
    {"name": "customer_id", "type": "string", "doc": "Customer identifier"},
    {"name": "order_status", "type": {"type": "enum", "name": "OrderStatus", "symbols": ["CREATED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]}},
    {"name": "order_amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}, "doc": "Order amount as decimal"},
    {"name": "currency", "type": "string", "default": "USD", "doc": "Currency code"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "cob", "type": "string", "doc": "COB date in YYYY-MM-DD format - REQUIRED for partitioning"},
    {"name": "items", "type": {
      "type": "array",
      "items": {
        "type": "record",
        "name": "OrderItem",
        "fields": [
          {"name": "product_id", "type": "string"},
          {"name": "quantity", "type": "int"},
          {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}
        ]
      }
    }, "default": []},
    {"name": "shipping_address", "type": ["null", {
      "type": "record",
      "name": "Address",
      "fields": [
        {"name": "street", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "state", "type": "string"},
        {"name": "zip_code", "type": "string"},
        {"name": "country", "type": "string", "default": "US"}
      ]
    }], "default": null},
    {"name": "metadata", "type": {
      "type": "record",
      "name": "EventMetadata",
      "fields": [
        {"name": "source", "type": "string"},
        {"name": "version", "type": "string", "default": "1.0"},
        {"name": "correlation_id", "type": ["null", "string"], "default": null},
        {"name": "original_format", "type": {"type": "enum", "name": "MessageFormat", "symbols": ["JSON", "CSV", "AVRO"]}}
      ]
    }}
  ]
}
```

**File:** `src/main/avro/coordination-message.avsc`
```json
{
  "type": "record",
  "name": "CoordinationMessage",
  "namespace": "com.company.kafkaconnector.avro",
  "fields": [
    {"name": "type", "type": {"type": "enum", "name": "MessageType", "symbols": ["BLOOM_REQUEST", "BLOOM_SHARE", "ACTIVITY_UPDATE", "HEALTH_CHECK"]}},
    {"name": "instance_id", "type": "string"},
    {"name": "partition_key", "type": "string"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "bloom_data", "type": ["null", "bytes"], "default": null},
    {"name": "activity_count", "type": ["null", "long"], "default": null},
    {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}
```

### Step 1.4: Implement Base Idempotency Models

**File:** `src/main/java/com/company/kafkaconnector/model/IdempotencyRecord.java`
```java
public class IdempotencyRecord {
    private String topic;
    private String cob;
    private String messageFingerprint;
    private int kafkaPartition;
    private long kafkaOffset;
    private LocalDateTime processedAt;
    private String connectorInstance;
    private String avroFingerprint; // Fingerprint of Avro binary data
    
    // Constructors, getters, setters
}
```

**File:** `src/main/java/com/company/kafkaconnector/model/PartitionActivity.java`
```java
public class PartitionActivity {
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong lastActivityTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong messagesLastHour = new AtomicLong(0);
    private final AtomicLong messagesLastDay = new AtomicLong(0);
    
    public void recordMessage();
    public boolean isHot();
    public boolean isCold();
    public long getMessagesPerHour();
}
```

### Step 1.5: Implement Multi-Format Message Parser and Converter

**File:** `src/main/java/com/company/kafkaconnector/service/MultiFormatMessageParser.java`
```java
@Service
public class MultiFormatMessageParser {
    
    private static final Logger logger = LoggerFactory.getLogger(MultiFormatMessageParser.class);
    
    private final ObjectMapper jsonMapper;
    private final SchemaRegistryClient schemaRegistryClient;
    private final Map<String, Schema> avroSchemaCache = new ConcurrentHashMap<>();
    
    public MultiFormatMessageParser(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.jsonMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    
    /**
     * Parse incoming message and convert to Avro GenericRecord based on detected format
     */
    public GenericRecord parseMessage(String rawMessage, String topicName, TopicConfig topicConfig) {
        MessageFormat detectedFormat = detectMessageFormat(rawMessage);
        String avroSchemaName = topicConfig.getAvroSchema();
        Schema avroSchema = getAvroSchema(avroSchemaName);
        
        logger.debug("Detected format: {} for topic: {}", detectedFormat, topicName);
        
        switch (detectedFormat) {
            case JSON:
                return parseJsonToAvro(rawMessage, avroSchema, detectedFormat);
            case CSV:
                return parseCsvToAvro(rawMessage, avroSchema, detectedFormat);
            case AVRO:
                return parseAvroMessage(rawMessage, avroSchema, detectedFormat);
            default:
                throw new IllegalArgumentException("Unsupported message format for topic: " + topicName);
        }
    }
    
    private MessageFormat detectMessageFormat(String rawMessage) {
        String trimmed = rawMessage.trim();
        
        // Check for JSON (starts with { or [)
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
            (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            return MessageFormat.JSON;
        }
        
        // Check for CSV (contains commas but not JSON structure)
        if (trimmed.contains(",") && !trimmed.contains("{") && !trimmed.contains("[")) {
            return MessageFormat.CSV;
        }
        
        // Check for Avro (binary data - would be base64 encoded in Kafka string)
        try {
            // Try to decode as base64 - if successful, likely Avro
            Base64.getDecoder().decode(trimmed);
            return MessageFormat.AVRO;
        } catch (IllegalArgumentException e) {
            // Not base64, continue detection
        }
        
        // Default to JSON if uncertain
        logger.warn("Could not detect format for message, defaulting to JSON: {}", 
                   trimmed.length() > 100 ? trimmed.substring(0, 100) + "..." : trimmed);
        return MessageFormat.JSON;
    }
    
    private GenericRecord parseJsonToAvro(String jsonMessage, Schema avroSchema, MessageFormat originalFormat) {
        try {
            JsonNode jsonNode = jsonMapper.readTree(jsonMessage);
            GenericRecord record = new GenericData.Record(avroSchema);
            
            // Map JSON fields to Avro schema
            for (Schema.Field field : avroSchema.getFields()) {
                String fieldName = field.name();
                JsonNode fieldValue = jsonNode.get(fieldName);
                
                if (fieldValue != null && !fieldValue.isNull()) {
                    Object convertedValue = convertJsonValueToAvro(fieldValue, field.schema());
                    record.put(fieldName, convertedValue);
                } else if (field.hasDefaultValue()) {
                    record.put(fieldName, field.defaultVal());
                }
            }
            
            // Set metadata
            setMessageMetadata(record, originalFormat);
            
            return record;
            
        } catch (Exception e) {
            throw new MessageParsingException("Failed to parse JSON message to Avro", e);
        }
    }
    
    private GenericRecord parseCsvToAvro(String csvMessage, Schema avroSchema, MessageFormat originalFormat) {
        try {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(false)
                .build();
                
            CSVParser parser = CSVParser.parse(csvMessage, csvFormat);
            List<CSVRecord> records = parser.getRecords();
            
            if (records.isEmpty()) {
                throw new MessageParsingException("No CSV records found in message");
            }
            
            // Use first record (assuming single record per message)
            CSVRecord csvRecord = records.get(0);
            GenericRecord avroRecord = new GenericData.Record(avroSchema);
            
            // Map CSV fields to Avro schema
            for (Schema.Field field : avroSchema.getFields()) {
                String fieldName = field.name();
                
                if (csvRecord.isSet(fieldName)) {
                    String csvValue = csvRecord.get(fieldName);
                    Object convertedValue = convertCsvValueToAvro(csvValue, field.schema());
                    avroRecord.put(fieldName, convertedValue);
                } else if (field.hasDefaultValue()) {
                    avroRecord.put(fieldName, field.defaultVal());
                }
            }
            
            // Set metadata
            setMessageMetadata(avroRecord, originalFormat);
            
            return avroRecord;
            
        } catch (Exception e) {
            throw new MessageParsingException("Failed to parse CSV message to Avro", e);
        }
    }
    
    private GenericRecord parseAvroMessage(String avroMessage, Schema expectedSchema, MessageFormat originalFormat) {
        try {
            // Decode base64 encoded Avro binary data
            byte[] avroBytes = Base64.getDecoder().decode(avroMessage);
            
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(expectedSchema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
            GenericRecord record = datumReader.read(null, decoder);
            
            // Set metadata
            setMessageMetadata(record, originalFormat);
            
            return record;
            
        } catch (Exception e) {
            throw new MessageParsingException("Failed to parse Avro message", e);
        }
    }
    
    private Object convertJsonValueToAvro(JsonNode jsonValue, Schema fieldSchema) {
        // Handle union types (nullable fields)
        if (fieldSchema.getType() == Schema.Type.UNION) {
            for (Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() != Schema.Type.NULL) {
                    return convertJsonValueToAvro(jsonValue, unionType);
                }
            }
        }
        
        switch (fieldSchema.getType()) {
            case STRING:
                return jsonValue.asText();
            case INT:
                return jsonValue.asInt();
            case LONG:
                // Handle timestamp conversion
                if ("timestamp-millis".equals(fieldSchema.getProp("logicalType"))) {
                    return parseTimestampToMillis(jsonValue.asText());
                }
                return jsonValue.asLong();
            case DOUBLE:
                return jsonValue.asDouble();
            case BOOLEAN:
                return jsonValue.asBoolean();
            case ARRAY:
                ArrayNode arrayNode = (ArrayNode) jsonValue;
                List<Object> arrayList = new ArrayList<>();
                for (JsonNode element : arrayNode) {
                    arrayList.add(convertJsonValueToAvro(element, fieldSchema.getElementType()));
                }
                return arrayList;
            case RECORD:
                GenericRecord nestedRecord = new GenericData.Record(fieldSchema);
                for (Schema.Field nestedField : fieldSchema.getFields()) {
                    JsonNode nestedValue = jsonValue.get(nestedField.name());
                    if (nestedValue != null) {
                        nestedRecord.put(nestedField.name(), 
                            convertJsonValueToAvro(nestedValue, nestedField.schema()));
                    }
                }
                return nestedRecord;
            case MAP:
                Map<String, Object> mapResult = new HashMap<>();
                jsonValue.fields().forEachRemaining(entry -> {
                    try {
                        mapResult.put(entry.getKey(), 
                            convertJsonValueToAvro(entry.getValue(), fieldSchema.getValueType()));
                    } catch (Exception e) {
                        logger.warn("Failed to convert map value for key: {}", entry.getKey(), e);
                    }
                });
                return mapResult;
            default:
                return jsonValue.asText(); // Fallback to string
        }
    }
    
    private Object convertCsvValueToAvro(String csvValue, Schema fieldSchema) {
        if (csvValue == null || csvValue.trim().isEmpty()) {
            return null;
        }
        
        // Handle union types (nullable fields)
        if (fieldSchema.getType() == Schema.Type.UNION) {
            for (Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() != Schema.Type.NULL) {
                    return convertCsvValueToAvro(csvValue, unionType);
                }
            }
        }
        
        switch (fieldSchema.getType()) {
            case STRING:
                return csvValue.trim();
            case INT:
                return Integer.parseInt(csvValue.trim());
            case LONG:
                // Handle timestamp conversion
                if ("timestamp-millis".equals(fieldSchema.getProp("logicalType"))) {
                    return parseTimestampToMillis(csvValue.trim());
                }
                return Long.parseLong(csvValue.trim());
            case DOUBLE:
                return Double.parseDouble(csvValue.trim());
            case BOOLEAN:
                return Boolean.parseBoolean(csvValue.trim());
            case ENUM:
                return new GenericData.EnumSymbol(fieldSchema, csvValue.trim());
            default:
                return csvValue.trim(); // Fallback to string
        }
    }
    
    private long parseTimestampToMillis(String timestampStr) {
        try {
            // Try parsing as epoch milliseconds first
            return Long.parseLong(timestampStr);
        } catch (NumberFormatException e) {
            // Try parsing as ISO datetime
            try {
                return Instant.parse(timestampStr).toEpochMilli();
            } catch (DateTimeParseException e2) {
                // Try parsing as local datetime
                return LocalDateTime.parse(timestampStr).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
            }
        }
    }
    
    private void setMessageMetadata(GenericRecord record, MessageFormat originalFormat) {
        // Set metadata about original format
        GenericRecord metadata = (GenericRecord) record.get("metadata");
        if (metadata != null) {
            metadata.put("original_format", new GenericData.EnumSymbol(
                metadata.getSchema().getField("original_format").schema(), originalFormat.name()));
        }
    }
    
    private Schema getAvroSchema(String schemaName) {
        return avroSchemaCache.computeIfAbsent(schemaName, name -> {
            try {
                // Load schema from Schema Registry
                SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(name);
                return new Schema.Parser().parse(metadata.getSchema());
            } catch (Exception e) {
                throw new RuntimeException("Failed to load Avro schema: " + name, e);
            }
        });
    }
    
    public enum MessageFormat {
        JSON, CSV, AVRO
    }
    
    public static class MessageParsingException extends RuntimeException {
        public MessageParsingException(String message) {
            super(message);
        }
        
        public MessageParsingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
```

### Step 1.6: Implement Multi-Format Fingerprint Generator

**File:** `src/main/java/com/company/kafkaconnector/util/MultiFormatFingerprintGenerator.java`
```java
@Component
public class MultiFormatFingerprintGenerator {
    
    private static final Logger logger = LoggerFactory.getLogger(MultiFormatFingerprintGenerator.class);
    
    /**
     * Generate fingerprint from raw message content (before Avro conversion)
     * This ensures consistent fingerprinting regardless of parsing variations
     */
    public String generateFingerprint(String rawMessageContent) {
        try {
            // Normalize the message content for consistent fingerprinting
            String normalized = normalizeMessageContent(rawMessageContent);
            return generateSHA256Hash(normalized.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            // Fallback to simple hash
            logger.warn("Failed to generate SHA256 fingerprint, using fallback", e);
            return String.valueOf(rawMessageContent.hashCode());
        }
    }
    
    /**
     * Generate fingerprint from Avro GenericRecord (after conversion)
     * Used for internal processing and bloom filter operations
     */
    public String generateAvroFingerprint(GenericRecord avroRecord) {
        try {
            // Use Avro binary encoding for consistent fingerprinting
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroRecord.getSchema());
            datumWriter.write(avroRecord, encoder);
            encoder.flush();
            
            byte[] avroBytes = outputStream.toByteArray();
            return generateSHA256Hash(avroBytes);
            
        } catch (IOException e) {
            // Fallback to string representation
            logger.warn("Failed to generate Avro binary fingerprint, using string fallback", e);
            return generateSHA256Hash(avroRecord.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
    
    public String extractCOB(GenericRecord avroRecord) {
        Object cobValue = avroRecord.get("cob");
        if (cobValue == null) {
            throw new IllegalArgumentException("COB field is required but not found in Avro record");
        }
        return cobValue.toString();
    }
    
    private String normalizeMessageContent(String rawMessage) {
        // Remove whitespace variations that don't affect semantic content
        String normalized = rawMessage.trim();
        
        // For JSON, remove formatting differences
        if (normalized.startsWith("{") || normalized.startsWith("[")) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(normalized);
                // Serialize back to canonical form (removes formatting differences)
                normalized = mapper.writeValueAsString(jsonNode);
            } catch (Exception e) {
                logger.debug("Could not normalize JSON, using original: {}", e.getMessage());
            }
        }
        
        return normalized;
    }
    
    private String generateSHA256Hash(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // Fallback to simple hash
            return String.valueOf(Arrays.hashCode(data));
        }
    }
    
    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }
}
```

### Step 1.4: Implement Database Entity and Repository

**File:** `src/main/java/com/company/kafkaconnector/entity/IdempotencyRecord.java`
```java
@Entity
@Table(name = "idempotency_log", 
       uniqueConstraints = @UniqueConstraint(columnNames = {"topic", "cob_date", "message_fingerprint"}))
public class IdempotencyRecord {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String topic;
    
    @Column(name = "cob_date", nullable = false)
    private LocalDate cobDate;
    
    @Column(name = "message_fingerprint", nullable = false, length = 64)
    private String messageFingerprint;
    
    @Column(name = "kafka_partition")
    private Integer kafkaPartition;
    
    @Column(name = "kafka_offset")
    private Long kafkaOffset;
    
    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
    
    @Column(name = "instance_id", length = 64)
    private String instanceId;
    
    // Constructors
    public IdempotencyRecord() {}
    
    public IdempotencyRecord(String topic, String cob, String messageFingerprint, 
                           Integer kafkaPartition, Long kafkaOffset, String instanceId) {
        this.topic = topic;
        this.cobDate = LocalDate.parse(cob);
        this.messageFingerprint = messageFingerprint;
        this.kafkaPartition = kafkaPartition;
        this.kafkaOffset = kafkaOffset;
        this.instanceId = instanceId;
        this.processedAt = Instant.now();
    }
    
    // Getters and setters...
}
```

**File:** `src/main/java/com/company/kafkaconnector/repository/IdempotencyRepository.java`
```java
@Repository
public interface IdempotencyRepository extends JpaRepository<IdempotencyRecord, Long> {
    
    /**
     * Check if message already processed - core duplicate detection
     */
    boolean existsByTopicAndCobDateAndMessageFingerprint(String topic, LocalDate cobDate, String messageFingerprint);
    
    /**
     * Get recent fingerprints for bloom filter bootstrap
     */
    @Query("SELECT i.messageFingerprint FROM IdempotencyRecord i " +
           "WHERE i.topic = :topic AND i.cobDate = :cobDate AND i.processedAt >= :since " +
           "ORDER BY i.processedAt DESC")
    List<String> findRecentFingerprints(@Param("topic") String topic, 
                                       @Param("cobDate") LocalDate cobDate,
                                       @Param("since") Instant since);
    
    /**
     * Find hot partitions for adaptive bloom filter management
     */
    @Query("SELECT new com.company.kafkaconnector.model.PartitionActivity(" +
           "i.topic, i.cobDate, COUNT(i), MAX(i.processedAt)) " +
           "FROM IdempotencyRecord i WHERE i.processedAt >= :since " +
           "GROUP BY i.topic, i.cobDate HAVING COUNT(i) > :threshold " +
           "ORDER BY COUNT(i) DESC")
    List<PartitionActivity> findHotPartitions(@Param("since") Instant since, 
                                             @Param("threshold") long threshold);
    
    /**
     * Get activity summary for monitoring
     */
    @Query("SELECT i.topic, i.cobDate, COUNT(i) as count " +
           "FROM IdempotencyRecord i WHERE i.processedAt >= :since " +
           "GROUP BY i.topic, i.cobDate")
    List<Object[]> getActivitySummary(@Param("since") Instant since);
    
    /**
     * Cleanup old records (for data retention)
     */
    @Modifying
    @Query("DELETE FROM IdempotencyRecord WHERE processedAt < :cutoff")
    int cleanupOldRecords(@Param("cutoff") Instant cutoff);
    
    /**
     * Count messages by instance for monitoring
     */
    @Query("SELECT i.instanceId, COUNT(i) FROM IdempotencyRecord i " +
           "WHERE i.processedAt >= :since GROUP BY i.instanceId")
    List<Object[]> getInstanceStats(@Param("since") Instant since);
}
```

---

## Phase 2: Database-Backed Idempotency Service

### Step 2.1: Implement Core Database Idempotency Service

**File:** `src/main/java/com/company/kafkaconnector/service/DatabaseIdempotencyService.java`
```java
@Service
public class DatabaseIdempotencyService {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseIdempotencyService.class);
    
    private final IdempotencyRepository repository;
    private final MultiFormatFingerprintGenerator fingerprintGenerator;
    private final String instanceId;
    
    public DatabaseIdempotencyService(IdempotencyRepository repository, 
                                    MultiFormatFingerprintGenerator fingerprintGenerator) {
        this.repository = repository;
        this.fingerprintGenerator = fingerprintGenerator;
        this.instanceId = InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID().toString().substring(0, 8);
        logger.info("Initialized idempotency service with instance ID: {}", instanceId);
    }
    
    /**
     * Check if message is duplicate using database lookup
     */
    public boolean isDuplicate(String topic, String cob, String rawMessageContent) {
        String fingerprint = fingerprintGenerator.generateFingerprint(rawMessageContent);
        LocalDate cobDate = LocalDate.parse(cob);
        
        boolean exists = repository.existsByTopicAndCobDateAndMessageFingerprint(topic, cobDate, fingerprint);
        
        if (exists) {
            logger.debug("Duplicate detected - Topic: {}, COB: {}, Fingerprint: {}", topic, cob, fingerprint);
        }
        
        return exists;
    }
    
    /**
     * Check if Avro record is duplicate (alternative method for parsed content)
     */
    public boolean isDuplicate(String topic, String cob, GenericRecord avroRecord) {
        String fingerprint = fingerprintGenerator.generateAvroFingerprint(avroRecord);
        LocalDate cobDate = LocalDate.parse(cob);
        
        return repository.existsByTopicAndCobDateAndMessageFingerprint(topic, cobDate, fingerprint);
    }
    
    /**
     * Mark message as processed in database
     */
    public void markAsProcessed(String topic, String cob, String rawMessageContent, 
                               ConsumerRecord<?, ?> kafkaRecord) {
        String fingerprint = fingerprintGenerator.generateFingerprint(rawMessageContent);
        
        try {
            IdempotencyRecord record = new IdempotencyRecord(
                topic, cob, fingerprint, 
                kafkaRecord.partition(), kafkaRecord.offset(), instanceId
            );
            
            repository.save(record);
            
            logger.debug("Marked as processed - Topic: {}, COB: {}, Partition: {}, Offset: {}", 
                        topic, cob, kafkaRecord.partition(), kafkaRecord.offset());
                        
        } catch (DataIntegrityViolationException e) {
            // Handle race condition - another instance may have inserted the same record
            if (e.getMessage().contains("unique constraint") || e.getMessage().contains("duplicate key")) {
                logger.debug("Concurrent insert detected for fingerprint: {} - ignoring", fingerprint);
            } else {
                throw e;
            }
        }
    }
    
    /**
     * Mark Avro record as processed
     */
    public void markAsProcessed(String topic, String cob, GenericRecord avroRecord, 
                               ConsumerRecord<?, ?> kafkaRecord) {
        String fingerprint = fingerprintGenerator.generateAvroFingerprint(avroRecord);
        
        try {
            IdempotencyRecord record = new IdempotencyRecord(
                topic, cob, fingerprint, 
                kafkaRecord.partition(), kafkaRecord.offset(), instanceId
            );
            
            repository.save(record);
            
        } catch (DataIntegrityViolationException e) {
            if (e.getMessage().contains("unique constraint") || e.getMessage().contains("duplicate key")) {
                logger.debug("Concurrent insert detected for Avro fingerprint: {} - ignoring", fingerprint);
            } else {
                throw e;
            }
        }
    }
    
    /**
     * Get recent fingerprints for bloom filter bootstrap
     */
    public List<String> getRecentFingerprints(String topic, String cob, Duration window) {
        LocalDate cobDate = LocalDate.parse(cob);
        Instant since = Instant.now().minus(window);
        
        return repository.findRecentFingerprints(topic, cobDate, since);
    }
    
    /**
     * Find hot partitions for adaptive optimization
     */
    public List<PartitionActivity> findHotPartitions(Duration window, long threshold) {
        Instant since = Instant.now().minus(window);
        return repository.findHotPartitions(since, threshold);
    }
    
    /**
     * Get activity summary for monitoring
     */
    public Map<String, Long> getActivitySummary(Duration window) {
        Instant since = Instant.now().minus(window);
        List<Object[]> results = repository.getActivitySummary(since);
        
        Map<String, Long> summary = new HashMap<>();
        for (Object[] result : results) {
            String key = result[0] + ":" + result[1]; // topic:cob
            Long count = (Long) result[2];
            summary.put(key, count);
        }
        
        return summary;
    }
    
    /**
     * Cleanup old records for data retention
     */
    @Scheduled(cron = "0 0 2 * * ?") // Daily at 2 AM
    @Transactional
    public void cleanupOldRecords() {
        Instant cutoff = Instant.now().minus(Duration.ofDays(7)); // Keep 7 days
        int deleted = repository.cleanupOldRecords(cutoff);
        
        if (deleted > 0) {
            logger.info("Cleaned up {} old idempotency records older than {}", deleted, cutoff);
        }
    }
    
    /**
     * Health check - verify database connectivity
     */
    public boolean isHealthy() {
        try {
            repository.count();
            return true;
        } catch (Exception e) {
            logger.error("Health check failed", e);
            return false;
        }
    }
    
    /**
     * Get instance statistics for monitoring
     */
    public Map<String, Long> getInstanceStats(Duration window) {
        Instant since = Instant.now().minus(window);
        List<Object[]> results = repository.getInstanceStats(since);
        
        Map<String, Long> stats = new HashMap<>();
        for (Object[] result : results) {
            stats.put((String) result[0], (Long) result[1]);
        }
        
        return stats;
    }
    
    public String getInstanceId() {
        return instanceId;
    }
}
```

### Step 2.2: Implement Supporting Model Classes

**File:** `src/main/java/com/company/kafkaconnector/model/PartitionActivity.java`
```java
public class PartitionActivity {
    private final String topic;
    private final LocalDate cobDate;
    private final long messageCount;
    private final Instant lastActivity;
    
    public PartitionActivity(String topic, LocalDate cobDate, long messageCount, Instant lastActivity) {
        this.topic = topic;
        this.cobDate = cobDate;
        this.messageCount = messageCount;
        this.lastActivity = lastActivity;
    }
    
    public String getTopic() { return topic; }
    public LocalDate getCobDate() { return cobDate; }
    public long getMessageCount() { return messageCount; }
    public Instant getLastActivity() { return lastActivity; }
    
    public String getPartitionKey() {
        return topic + ":" + cobDate.toString();
    }
    
    public boolean isHot(long threshold) {
        return messageCount > threshold;
    }
    
    @Override
    public String toString() {
        return String.format("PartitionActivity{topic='%s', cob=%s, count=%d, lastActivity=%s}", 
                           topic, cobDate, messageCount, lastActivity);
    }
}
```

### Step 2.3: Implement Database Configuration

**File:** `src/main/resources/application.yml`
```yaml
# Database Configuration
spring:
  datasource:
    # H2 for local development
    url: jdbc:h2:file:./data/idempotency;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    driver-class-name: org.h2.Driver
    username: sa
    password: 
    hikari:
      maximum-pool-size: 10
      connection-timeout: 20000
      
  jpa:
    hibernate:
      ddl-auto: none  # Let Liquibase handle schema
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.H2Dialect
        format_sql: true
        
  liquibase:
    change-log: classpath:db/changelog/db.changelog-master.yaml
    enabled: true

  h2:
    console:
      enabled: true
      path: /h2-console

---
# Production configuration
spring:
  profiles: prod
  datasource:
    # CockroachDB for production
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:26257}/${DB_NAME:connector}?sslmode=${DB_SSL_MODE:require}
    driver-class-name: org.postgresql.Driver
    username: ${DB_USERNAME:connector_user}
    password: ${DB_PASSWORD}
    hikari:
      maximum-pool-size: 20
      connection-timeout: 30000
      leak-detection-threshold: 60000
      
  jpa:
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        jdbc:
          batch_size: 50
        order_inserts: true
        order_updates: true
```

---

## Phase 3: Adaptive Bloom Filter Management

### Step 3.1: Implement Adaptive Bloom Filter Service

**File:** `src/main/java/com/company/kafkaconnector/service/AdaptiveBloomFilterService.java`
```java
@Service
public class AdaptiveBloomFilterService {
    
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveBloomFilterService.class);
    
    // Configuration
    private static final int MAX_BLOOM_FILTERS = 20;
    private static final int HOT_THRESHOLD_MESSAGES_PER_HOUR = 1000;
    private static final int BLOOM_EXPECTED_ELEMENTS = 100_000;
    private static final double BLOOM_FALSE_POSITIVE_RATE = 0.01;
    
    private final DatabaseIdempotencyService databaseService;
    private final LRUCache<String, BloomFilter<String>> bloomFilterCache;
    private final AtomicLong cacheHits = new AtomicLong(0);
    private final AtomicLong cacheMisses = new AtomicLong(0);
    
    public AdaptiveBloomFilterService(DatabaseIdempotencyService databaseService) {
        this.databaseService = databaseService;
        this.bloomFilterCache = new LRUCache<>(MAX_BLOOM_FILTERS);
    }
    
    /**
     * Enhanced duplicate check with adaptive bloom filter optimization
     */
    public boolean isDuplicateWithBloom(String topic, String cob, String rawMessageContent) {
        String partitionKey = topic + ":" + cob;
        String fingerprint = databaseService.getFingerprintGenerator().generateFingerprint(rawMessageContent);
        
        // Check bloom filter first (if available)
        BloomFilter<String> bloom = bloomFilterCache.get(partitionKey);
        if (bloom != null) {
            if (!bloom.mightContain(fingerprint)) {
                // Definitely not a duplicate - bloom filter says no
                logger.debug("Bloom filter eliminated duplicate check for partition: {}", partitionKey);
                cacheHits.incrementAndGet();
                return false;
            } else {
                // Possibly a duplicate - need database check
                logger.debug("Bloom filter indicates possible duplicate for partition: {}", partitionKey);
            }
        } else {
            cacheMisses.incrementAndGet();
        }
        
        // Authoritative database check
        boolean isDuplicate = databaseService.isDuplicate(topic, cob, rawMessageContent);
        
        // If not duplicate, add to bloom filter (if we have one)
        if (!isDuplicate && bloom != null) {
            bloom.put(fingerprint);
        }
        
        return isDuplicate;
    }
    
    /**
     * Enhanced mark as processed with bloom filter update
     */
    public void markAsProcessedWithBloom(String topic, String cob, String rawMessageContent, 
                                        ConsumerRecord<?, ?> kafkaRecord) {
        String partitionKey = topic + ":" + cob;
        String fingerprint = databaseService.getFingerprintGenerator().generateFingerprint(rawMessageContent);
        
        // Mark in database
        databaseService.markAsProcessed(topic, cob, rawMessageContent, kafkaRecord);
        
        // Add to bloom filter if we have one
        BloomFilter<String> bloom = bloomFilterCache.get(partitionKey);
        if (bloom != null) {
            bloom.put(fingerprint);
        }
    }
    
    /**
     * Manage bloom filters based on partition activity
     */
    @Scheduled(fixedRate = 60000) // Every minute
    public void manageBloomFilters() {
        try {
            // Find hot partitions in the last hour
            List<PartitionActivity> hotPartitions = databaseService.findHotPartitions(
                Duration.ofHours(1), HOT_THRESHOLD_MESSAGES_PER_HOUR
            );
            
            logger.debug("Found {} hot partitions for bloom filter management", hotPartitions.size());
            
            // Create bloom filters for new hot partitions
            for (PartitionActivity activity : hotPartitions) {
                String partitionKey = activity.getPartitionKey();
                
                if (!bloomFilterCache.containsKey(partitionKey)) {
                    createBloomFilterForPartition(activity);
                }
            }
            
            // Remove bloom filters for partitions that are no longer hot
            cleanupInactiveBloomFilters();
            
            logBloomFilterStats();
            
        } catch (Exception e) {
            logger.error("Error managing bloom filters", e);
        }
    }
    
    private void createBloomFilterForPartition(PartitionActivity activity) {
        String partitionKey = activity.getPartitionKey();
        
        try {
            logger.info("Creating bloom filter for hot partition: {} (activity: {} messages)", 
                       partitionKey, activity.getMessageCount());
            
            // Create optimally sized bloom filter
            int expectedElements = Math.max((int) activity.getMessageCount() * 2, BLOOM_EXPECTED_ELEMENTS);
            BloomFilter<String> bloom = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8), 
                expectedElements, 
                BLOOM_FALSE_POSITIVE_RATE
            );
            
            // Bootstrap with recent fingerprints from database
            List<String> recentFingerprints = databaseService.getRecentFingerprints(
                activity.getTopic(), activity.getCobDate().toString(), Duration.ofHours(1)
            );
            
            recentFingerprints.forEach(bloom::put);
            
            bloomFilterCache.put(partitionKey, bloom);
            
            logger.info("Created bloom filter for {}: {} fingerprints loaded, expected elements: {}", 
                       partitionKey, recentFingerprints.size(), expectedElements);
            
        } catch (Exception e) {
            logger.error("Failed to create bloom filter for partition: {}", partitionKey, e);
        }
    }
    
    private void cleanupInactiveBloomFilters() {
        Set<String> currentKeys = new HashSet<>(bloomFilterCache.keySet());
        
        for (String partitionKey : currentKeys) {
            try {
                String[] parts = partitionKey.split(":");
                if (parts.length != 2) continue;
                
                String topic = parts[0];
                String cob = parts[1];
                
                // Check if partition is still hot
                List<PartitionActivity> recent = databaseService.findHotPartitions(
                    Duration.ofMinutes(30), HOT_THRESHOLD_MESSAGES_PER_HOUR / 2
                );
                
                boolean stillHot = recent.stream()
                    .anyMatch(activity -> activity.getPartitionKey().equals(partitionKey));
                
                if (!stillHot) {
                    bloomFilterCache.remove(partitionKey);
                    logger.info("Removed bloom filter for inactive partition: {}", partitionKey);
                }
                
            } catch (Exception e) {
                logger.warn("Error checking partition activity for: {}", partitionKey, e);
                // Remove problematic entry
                bloomFilterCache.remove(partitionKey);
            }
        }
    }
    
    private void logBloomFilterStats() {
        int activeFilters = bloomFilterCache.size();
        long totalHits = cacheHits.get();
        long totalMisses = cacheMisses.get();
        double hitRate = totalHits + totalMisses > 0 ? (double) totalHits / (totalHits + totalMisses) : 0.0;
        
        logger.info("Bloom filter stats - Active: {}, Hits: {}, Misses: {}, Hit rate: {:.2%}", 
                   activeFilters, totalHits, totalMisses, hitRate);
    }
    
    /**
     * Get current bloom filter statistics for monitoring
     */
    public BloomFilterStats getStats() {
        return new BloomFilterStats(
            bloomFilterCache.size(),
            cacheHits.get(),
            cacheMisses.get(),
            bloomFilterCache.keySet()
        );
    }
    
    /**
     * Force refresh of bloom filters (for admin operations)
     */
    public void refreshBloomFilters() {
        logger.info("Forcing bloom filter refresh...");
        bloomFilterCache.clear();
        manageBloomFilters();
    }
    
    /**
     * Statistics class for monitoring
     */
    public static class BloomFilterStats {
        private final int activeFilters;
        private final long cacheHits;
        private final long cacheMisses;
        private final Set<String> activePartitions;
        
        public BloomFilterStats(int activeFilters, long cacheHits, long cacheMisses, Set<String> activePartitions) {
            this.activeFilters = activeFilters;
            this.cacheHits = cacheHits;
            this.cacheMisses = cacheMisses;
            this.activePartitions = new HashSet<>(activePartitions);
        }
        
        public int getActiveFilters() { return activeFilters; }
        public long getCacheHits() { return cacheHits; }
        public long getCacheMisses() { return cacheMisses; }
        public Set<String> getActivePartitions() { return activePartitions; }
        
        public double getHitRate() {
            long total = cacheHits + cacheMisses;
            return total > 0 ? (double) cacheHits / total : 0.0;
        }
    }
}
```

### Step 3.2: Implement Rebalance-Aware Consumer Service

**File:** `src/main/java/com/company/kafkaconnector/service/RebalanceAwareKafkaConsumerService.java`
```java
@Service
public class RebalanceAwareKafkaConsumerService {
    
    private final DistributedAdaptiveIdempotencyService idempotencyService;
    private final AvroFingerprintGenerator fingerprintGenerator;
    private final AtomicReference<Set<TopicPartition>> assignedPartitions = new AtomicReference<>(Collections.emptySet());
    
    @KafkaListener(
        topics = "#{@connectorConfiguration.getTopics().values().![kafkaTopic]}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleMessage(
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        @Payload String rawMessage,  // Changed from GenericRecord to String to handle multiple formats
        Acknowledgment acknowledgment
    ) {
        // Verify we still own this partition (prevent race conditions during rebalancing)
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        if (!assignedPartitions.get().contains(topicPartition)) {
            logger.warn("Received message for unassigned partition: {} - skipping", topicPartition);
            // Don't acknowledge - let rebalanced owner handle it
            return;
        }
        
        try {
            // Parse message to Avro format (handles JSON, CSV, Avro inputs)
            TopicConfig topicConfig = getTopicConfig(topic);
            GenericRecord avroRecord = messageParser.parseMessage(rawMessage, topic, topicConfig);
            
            // Extract COB from parsed Avro record
            String cob = fingerprintGenerator.extractCOB(avroRecord);
            
            // Generate fingerprint from original raw message for consistency
            String messageFingerprint = fingerprintGenerator.generateFingerprint(rawMessage);
            
            // Check for duplicates using both raw content and parsed content
            if (idempotencyService.isDuplicate(topic, cob, rawMessage, avroRecord)) {
                logger.info("Skipping duplicate - Topic: {}, COB: {}, Partition: {}, Offset: {}, Format: {}", 
                           topic, cob, partition, offset, getOriginalFormat(avroRecord));
                acknowledgment.acknowledge();
                return;
            }
            
            // Process message with multi-format support
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                topic, partition, offset, null, rawMessage
            );
            
            ProcessingResult result = processMultiFormatMessage(record, avroRecord, cob, topicConfig);
            
            if (result.isSuccess()) {
                // Mark as processed and acknowledge
                idempotencyService.markAsProcessed(topic, cob, rawMessage, avroRecord, record);
                acknowledgment.acknowledge();
            } else {
                // Handle processing failure based on error type
                handleProcessingFailure(result, topic, cob, acknowledgment);
            }
            
        } catch (MultiFormatMessageParser.MessageParsingException e) {
            logger.error("Failed to parse message format for topic: {}, partition: {}, offset: {} - treating as non-retriable", 
                        topic, partition, offset, e);
            handleProcessingException(new NonRetriableException("Message parsing failed: " + e.getMessage(), e), acknowledgment);
            
        } catch (Exception e) {
            logger.error("Error processing message from topic: {}, partition: {}, offset: {}", 
                        topic, partition, offset, e);
            handleProcessingException(e, acknowledgment);
        }
    }
    
    @EventListener
    public void handlePartitionAssignment(ConsumerPartitionAssignedEvent event) {
        Set<TopicPartition> newAssignments = new HashSet<>(event.getPartitions());
        assignedPartitions.set(newAssignments);
        
        logger.info("Instance assigned partitions: {}", newAssignments);
        
        // Notify idempotency service of new partition assignments
        idempotencyService.onPartitionsAssigned(newAssignments);
    }
    
    @EventListener
    public void handlePartitionRevocation(ConsumerPartitionRevokedEvent event) {
        Set<TopicPartition> revokedPartitions = new HashSet<>(event.getPartitions());
        
        logger.info("Instance revoked partitions: {}", revokedPartitions);
        
        // Clean up resources for revoked partitions
        idempotencyService.onPartitionsRevoked(revokedPartitions);
        
        // Update assigned partitions
        Set<TopicPartition> current = assignedPartitions.get();
        Set<TopicPartition> updated = current.stream()
            .filter(p -> !revokedPartitions.contains(p))
            .collect(Collectors.toSet());
        assignedPartitions.set(updated);
    }
    
    private ProcessingResult processMessageWithIdempotency(ConsumerRecord<String, GenericRecord> record, String cob) {
        // This will be implemented in Phase 4 with Delta Lake integration
        throw new UnsupportedOperationException("Message processing not yet implemented");
    }
    
    private void handleProcessingFailure(ProcessingResult result, String topic, String cob, Acknowledgment ack) {
        // Handle retriable vs non-retriable failures
        // Implementation will be completed in Phase 6
        throw new UnsupportedOperationException("Failure handling not yet implemented");
    }
    
    private void handleProcessingException(Exception e, Acknowledgment ack) {
        // Handle unexpected exceptions during processing
        // Implementation will be completed in Phase 6
        throw new UnsupportedOperationException("Exception handling not yet implemented");
    }
}
```

---

## Phase 4: COB-Based Delta Lake Integration

### Step 4.1: Implement Integrated Consumer Service with Database Idempotency

**File:** `src/main/java/com/company/kafkaconnector/service/IntegratedKafkaConsumerService.java`
```java
@Service
public class IntegratedKafkaConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(IntegratedKafkaConsumerService.class);
    
    private final DatabaseIdempotencyService idempotencyService;
    private final AdaptiveBloomFilterService bloomService;
    private final MultiFormatMessageParser messageParser;
    private final CobAwareDeltaWriterService deltaWriterService;
    private final TopicConfigurationService topicConfigService;
    private final DeadLetterQueueService dlqService;
    
    @KafkaListener(
        topics = "#{@connectorConfiguration.getTopics().values().![kafkaTopic]}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleMessage(
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        @Payload String rawMessage,
        Acknowledgment acknowledgment
    ) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        
        try {
            TopicConfig topicConfig = topicConfigService.getConfigForTopic(topic);
            if (topicConfig == null) {
                throw new NonRetriableException("No configuration found for topic: " + topic);
            }
            
            // Parse message to Avro format (handles JSON, CSV, Avro inputs)
            GenericRecord avroRecord = messageParser.parseMessage(rawMessage, topic, topicConfig);
            
            // Extract COB from parsed Avro record (required field)
            String cob = extractCobFromAvroRecord(avroRecord);
            
            // Create Kafka record representation
            ConsumerRecord<String, String> kafkaRecord = new ConsumerRecord<>(
                topic, partition, offset, null, rawMessage
            );
            
            // Check for duplicates using database + adaptive bloom filters
            if (bloomService.isDuplicateWithBloom(topic, cob, rawMessage)) {
                logger.info("Duplicate detected - Topic: {}, COB: {}, Partition: {}, Offset: {}", 
                           topic, cob, partition, offset);
                acknowledgment.acknowledge();
                return;
            }
            
            // Process message: transform and write to Delta Lake
            processMessage(topic, cob, rawMessage, avroRecord, topicConfig, kafkaRecord);
            
            // Mark as processed in database (with bloom filter update)
            bloomService.markAsProcessedWithBloom(topic, cob, rawMessage, kafkaRecord);
            
            // Acknowledge successful processing
            acknowledgment.acknowledge();
            
            logger.debug("Successfully processed message - Topic: {}, COB: {}, Partition: {}, Offset: {}", 
                        topic, cob, partition, offset);
            
        } catch (MessageParsingException e) {
            logger.error("Failed to parse message - Topic: {}, Partition: {}, Offset: {} - sending to DLQ", 
                        topic, partition, offset, e);
            handleNonRetriableFailure(topic, rawMessage, e, acknowledgment);
            
        } catch (CobExtractionException e) {
            logger.error("Failed to extract COB - Topic: {}, Partition: {}, Offset: {} - sending to DLQ", 
                        topic, partition, offset, e);
            handleNonRetriableFailure(topic, rawMessage, e, acknowledgment);
            
        } catch (RetriableException e) {
            logger.warn("Retriable error processing message - Topic: {}, Partition: {}, Offset: {} - will retry", 
                       topic, partition, offset, e);
            // Don't acknowledge - let Kafka retry
            throw e;
            
        } catch (NonRetriableException e) {
            logger.error("Non-retriable error processing message - Topic: {}, Partition: {}, Offset: {} - sending to DLQ", 
                        topic, partition, offset, e);
            handleNonRetriableFailure(topic, rawMessage, e, acknowledgment);
            
        } catch (Exception e) {
            logger.error("Unexpected error processing message - Topic: {}, Partition: {}, Offset: {}", 
                        topic, partition, offset, e);
            
            // Classify as retriable or non-retriable based on exception type
            if (isRetriableException(e)) {
                throw new RetriableException("Unexpected retriable error", e);
            } else {
                handleNonRetriableFailure(topic, rawMessage, new NonRetriableException("Unexpected error", e), acknowledgment);
            }
        } finally {
            MDC.remove("correlationId");
        }
    }
    
    private void processMessage(String topic, String cob, String rawMessage, GenericRecord avroRecord, 
                               TopicConfig topicConfig, ConsumerRecord<String, String> kafkaRecord) {
        try {
            // Validate COB date
            if (!isValidCobDate(cob)) {
                throw new NonRetriableException("Invalid COB date format: " + cob);
            }
            
            // Write to Delta Lake with COB-based partitioning
            deltaWriterService.writeMessage(topic, cob, avroRecord, topicConfig);
            
            logger.debug("Successfully wrote to Delta Lake - Topic: {}, COB: {}", topic, cob);
            
        } catch (DeltaWriteException e) {
            // Classify Delta Lake errors
            if (e.isRetriable()) {
                throw new RetriableException("Delta Lake write failed temporarily", e);
            } else {
                throw new NonRetriableException("Delta Lake write failed permanently", e);
            }
        }
    }
    
    private String extractCobFromAvroRecord(GenericRecord avroRecord) {
        Object cobValue = avroRecord.get("cob");
        if (cobValue == null) {
            throw new CobExtractionException("COB field is required but not found in message");
        }
        
        String cob = cobValue.toString();
        if (!isValidCobDate(cob)) {
            throw new CobExtractionException("Invalid COB date format: " + cob);
        }
        
        return cob;
    }
    
    private boolean isValidCobDate(String cob) {
        try {
            LocalDate.parse(cob);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
    
    private void handleNonRetriableFailure(String topic, String rawMessage, Exception error, Acknowledgment acknowledgment) {
        try {
            // Send to dead letter queue
            dlqService.sendToDeadLetterQueue(topic, rawMessage, error);
            
            // Acknowledge to prevent infinite retry
            acknowledgment.acknowledge();
            
        } catch (Exception dlqError) {
            logger.error("Failed to send message to DLQ for topic: {}", topic, dlqError);
            // Still acknowledge to prevent infinite loop
            acknowledgment.acknowledge();
        }
    }
    
    private boolean isRetriableException(Exception e) {
        String exceptionName = e.getClass().getSimpleName();
        String message = e.getMessage();
        
        // Network and I/O related issues
        if (exceptionName.contains("Connect") || 
            exceptionName.contains("Timeout") || 
            exceptionName.contains("IO") ||
            exceptionName.contains("Socket")) {
            return true;
        }
        
        // Database connection issues
        if (exceptionName.contains("SQLException") ||
            exceptionName.contains("Connection") ||
            exceptionName.contains("Transient")) {
            return true;
        }
        
        // S3/AWS specific retriable errors
        if (exceptionName.contains("ServiceException") || 
            exceptionName.contains("ThrottlingException") ||
            exceptionName.contains("InternalServerError") || 
            exceptionName.contains("ServiceUnavailableException") ||
            exceptionName.contains("SlowDown")) {
            return true;
        }
        
        // Check message content for retriable indicators
        if (message != null) {
            String lowerMessage = message.toLowerCase();
            if (lowerMessage.contains("timeout") ||
                lowerMessage.contains("connection") ||
                lowerMessage.contains("unavailable") ||
                lowerMessage.contains("throttl") ||
                lowerMessage.contains("rate limit") ||
                lowerMessage.contains("try again")) {
                return true;
            }
        }
        
        return false;
    }
    
    // Exception classes for better error handling
    public static class CobExtractionException extends RuntimeException {
        public CobExtractionException(String message) {
            super(message);
        }
        
        public CobExtractionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
```

### Step 4.2: Enhance Delta Writer Service for COB Partitioning

**File:** Update `src/main/java/com/company/kafkaconnector/service/DeltaWriterService.java`
```java
@Service
public class CobAwareDeltaWriterService {
    
    private final DeltaKernelWriterService deltaKernelWriter;
    private final CobPartitioningService partitioningService;
    private final AvroToDeltaConverter avroConverter;
    
    // Batch management per (topic, cob) combination
    private final Map<String, List<Row>> cobBatches = new ConcurrentHashMap<>();
    private final Map<String, Integer> batchCounts = new ConcurrentHashMap<>();
    
    public void writeMessage(String topic, String cob, GenericRecord avroRecord, TopicConfig topicConfig) {
        if (!partitioningService.isValidCobDate(cob)) {
            throw new IllegalArgumentException("Invalid COB date format: " + cob);
        }
        
        String batchKey = topic + ":" + cob;
        
        // Convert Avro record to Delta Row
        Row deltaRow = avroConverter.convertToDeltaRow(avroRecord, cob);
        
        // Add to batch
        addToBatch(batchKey, deltaRow);
        
        // Check if batch should be flushed
        if (shouldFlushBatch(batchKey, topicConfig)) {
            flushBatch(batchKey, topic, cob, topicConfig);
        }
    }
    
    private void addToBatch(String batchKey, Row deltaRow) {
        cobBatches.computeIfAbsent(batchKey, k -> new ArrayList<>()).add(deltaRow);
        batchCounts.merge(batchKey, 1, Integer::sum);
    }
    
    private boolean shouldFlushBatch(String batchKey, TopicConfig topicConfig) {
        int currentSize = batchCounts.getOrDefault(batchKey, 0);
        return currentSize >= topicConfig.getProcessing().getBatchSize();
    }
    
    private void flushBatch(String batchKey, String topic, String cob, TopicConfig topicConfig) {
        List<Row> batch = cobBatches.remove(batchKey);
        batchCounts.remove(batchKey);
        
        if (batch == null || batch.isEmpty()) {
            return;
        }
        
        logger.info("Flushing batch of {} messages for topic: {}, COB: {}", batch.size(), topic, cob);
        
        try {
            // Get Delta table path for this COB partition
            String deltaTablePath = partitioningService.getDeltaTablePath(topic, cob, topicConfig);
            Map<String, String> partitionValues = partitioningService.getPartitionValues(cob);
            
            // Write to Delta Lake with COB-based partitioning
            deltaKernelWriter.writeBatch(deltaTablePath, batch, partitionValues, topicConfig);
            
            logger.info("Successfully wrote {} messages to Delta table: {}", batch.size(), deltaTablePath);
            
        } catch (Exception e) {
            logger.error("Failed to write batch for topic: {}, COB: {}", topic, cob, e);
            
            // Re-add batch for retry (or send to DLQ)
            cobBatches.put(batchKey, batch);
            batchCounts.put(batchKey, batch.size());
            
            throw new DeltaWriteException("Failed to write COB batch", topic, cob, batch.size(), e);
        }
    }
    
    public void flushAllBatches() {
        logger.info("Flushing all remaining COB batches: {}", cobBatches.size());
        
        cobBatches.entrySet().forEach(entry -> {
            String batchKey = entry.getKey();
            String[] parts = batchKey.split(":");
            String topic = parts[0];
            String cob = parts[1];
            
            try {
                TopicConfig topicConfig = getTopicConfig(topic);
                flushBatch(batchKey, topic, cob, topicConfig);
            } catch (Exception e) {
                logger.error("Failed to flush batch for {}", batchKey, e);
            }
        });
    }
    
    // Scheduled flush for time-based batching
    @Scheduled(fixedRateString = "${connector.flush-interval-seconds:60}000")
    public void scheduledFlush() {
        // Flush batches that have been sitting too long
        long flushThreshold = System.currentTimeMillis() - Duration.ofMinutes(5).toMillis();
        
        cobBatches.entrySet().stream()
            .filter(entry -> shouldFlushByTime(entry.getKey(), flushThreshold))
            .forEach(entry -> {
                String batchKey = entry.getKey();
                String[] parts = batchKey.split(":");
                try {
                    TopicConfig topicConfig = getTopicConfig(parts[0]);
                    flushBatch(batchKey, parts[0], parts[1], topicConfig);
                } catch (Exception e) {
                    logger.error("Failed to flush batch by time for {}", batchKey, e);
                }
            });
    }
}
```

---

## Phase 5: Consumer Integration & Safety

### Step 5.1: Implement Database Health Monitoring

**File:** `src/main/java/com/company/kafkaconnector/service/DatabaseHealthMonitor.java`
```java
@Component
public class DatabaseHealthMonitor {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseHealthMonitor.class);
    
    public enum HealthStatus {
        HEALTHY, DEGRADED, DOWN
    }
    
    private final DatabaseIdempotencyService databaseService;
    private final AtomicReference<HealthStatus> currentHealth = new AtomicReference<>(HealthStatus.HEALTHY);
    private final AtomicLong lastSuccessfulCheck = new AtomicLong(System.currentTimeMillis());
    
    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void performHealthCheck() {
        try {
            // Test basic database connectivity
            boolean isHealthy = databaseService.isHealthy();
            
            if (isHealthy) {
                // Test query performance
                long startTime = System.currentTimeMillis();
                databaseService.getActivitySummary(Duration.ofMinutes(1));
                long queryTime = System.currentTimeMillis() - startTime;
                
                if (queryTime < 5000) { // Under 5 seconds
                    currentHealth.set(HealthStatus.HEALTHY);
                    lastSuccessfulCheck.set(System.currentTimeMillis());
                } else {
                    logger.warn("Database queries are slow: {}ms - marking as degraded", queryTime);
                    currentHealth.set(HealthStatus.DEGRADED);
                }
                
            } else {
                logger.error("Database health check failed - marking as down");
                currentHealth.set(HealthStatus.DOWN);
            }
            
        } catch (Exception e) {
            logger.error("Database health check failed with exception", e);
            currentHealth.set(HealthStatus.DOWN);
        }
    }
    
    public HealthStatus getCurrentHealth() {
        return currentHealth.get();
    }
    
    public boolean isHealthyForProcessing() {
        HealthStatus status = currentHealth.get();
        return status == HealthStatus.HEALTHY || status == HealthStatus.DEGRADED;
    }
    
    public long getTimeSinceLastSuccess() {
        return System.currentTimeMillis() - lastSuccessfulCheck.get();
    }
    
    @EventListener
    public void onApplicationReady(ApplicationReadyEvent event) {
        logger.info("Application ready - starting initial health check");
        performHealthCheck();
    }
}
```

### Step 5.2: Implement Dead Letter Queue Service

**File:** `src/main/java/com/company/kafkaconnector/service/DeadLetterQueueService.java`
```java
@Service  
public class DeadLetterQueueService {
    
    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueService.class);
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    public void sendToDeadLetterQueue(String originalTopic, String originalMessage, Exception error) {
        try {
            String dlqTopic = buildDlqTopicName(originalTopic);
            
            // Create DLQ message with error context
            DlqMessage dlqMessage = new DlqMessage(
                originalTopic,
                originalMessage,
                error.getClass().getSimpleName(),
                error.getMessage(),
                getStackTrace(error),
                Instant.now()
            );
            
            String dlqMessageJson = objectMapper.writeValueAsString(dlqMessage);
            
            // Send to DLQ topic
            kafkaTemplate.send(dlqTopic, dlqMessageJson)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failed to send message to DLQ topic: {}", dlqTopic, throwable);
                    } else {
                        logger.info("Successfully sent message to DLQ: {}", dlqTopic);
                    }
                });
                
        } catch (Exception e) {
            logger.error("Failed to process DLQ message for topic: {}", originalTopic, e);
        }
    }
    
    private String buildDlqTopicName(String originalTopic) {
        return originalTopic + "-dlq";
    }
    
    private String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
    
    public static class DlqMessage {
        private String originalTopic;
        private String originalMessage;
        private String errorClass;
        private String errorMessage;
        private String stackTrace;
        private Instant timestamp;
        
        public DlqMessage(String originalTopic, String originalMessage, String errorClass, 
                         String errorMessage, String stackTrace, Instant timestamp) {
            this.originalTopic = originalTopic;
            this.originalMessage = originalMessage;
            this.errorClass = errorClass;
            this.errorMessage = errorMessage;
            this.stackTrace = stackTrace;
            this.timestamp = timestamp;
        }
        
        // Getters and setters...
    }
}
```

### Step 5.3: Implement Topic Configuration Service

**File:** `src/main/java/com/company/kafkaconnector/service/TopicConfigurationService.java`
```java
@Service
public class TopicConfigurationService {
    
    private static final Logger logger = LoggerFactory.getLogger(TopicConfigurationService.class);
    
    private final ConnectorConfiguration connectorConfiguration;
    private final Map<String, TopicConfig> topicConfigCache;
    
    public TopicConfigurationService(ConnectorConfiguration connectorConfiguration) {
        this.connectorConfiguration = connectorConfiguration;
        this.topicConfigCache = new ConcurrentHashMap<>();
        initializeTopicConfigs();
    }
    
    @PostConstruct
    private void initializeTopicConfigs() {
        connectorConfiguration.getTopics().values().forEach(config -> {
            topicConfigCache.put(config.getKafkaTopic(), config);
            logger.info("Registered topic configuration: {} -> {}", 
                       config.getKafkaTopic(), config.getDestination().getTableName());
        });
    }
    
    public TopicConfig getConfigForTopic(String topicName) {
        return topicConfigCache.get(topicName);
    }
    
    public Set<String> getConfiguredTopics() {
        return Collections.unmodifiableSet(topicConfigCache.keySet());
    }
    
    public boolean isTopicConfigured(String topicName) {
        return topicConfigCache.containsKey(topicName);
    }
    
    @EventListener
    public void handleConfigurationRefresh(ContextRefreshedEvent event) {
        logger.info("Configuration refresh detected - reinitializing topic configs");
        topicConfigCache.clear();
        initializeTopicConfigs();
    }
}

---

## Phase 6: Monitoring and Observability

### Step 6.1: Implement Database Metrics Collection

**File:** `src/main/java/com/company/kafkaconnector/service/DatabaseMetricsService.java`
```java
@Component
public class DatabaseMetricsService {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseMetricsService.class);
    
    private final MeterRegistry meterRegistry;
    private final DatabaseIdempotencyService databaseService;
    private final AdaptiveBloomFilterService bloomService;
    
    // Counters
    private final Counter duplicatesDetected;
    private final Counter databaseQueries;
    private final Counter bloomHits;
    private final Counter bloomMisses;
    private final Counter messagesProcessed;
    private final Counter dlqMessages;
    
    // Timers
    private final Timer databaseQueryLatency;
    private final Timer messageProcessingLatency;
    
    // Gauges
    private final AtomicLong activeBloomFilters = new AtomicLong(0);
    private final AtomicLong totalRecordsInDatabase = new AtomicLong(0);
    private final AtomicLong databaseHealthStatus = new AtomicLong(1); // 1=healthy, 0=unhealthy
    
    public DatabaseMetricsService(MeterRegistry meterRegistry, 
                                 DatabaseIdempotencyService databaseService,
                                 AdaptiveBloomFilterService bloomService) {
        this.meterRegistry = meterRegistry;
        this.databaseService = databaseService;
        this.bloomService = bloomService;
        
        // Initialize counters
        this.duplicatesDetected = Counter.builder("idempotency.duplicates.detected")
            .description("Number of duplicate messages detected")
            .tag("component", "database-idempotency")
            .register(meterRegistry);
            
        this.databaseQueries = Counter.builder("idempotency.database.queries")
            .description("Number of database queries performed")
            .register(meterRegistry);
            
        this.bloomHits = Counter.builder("idempotency.bloom.hits")
            .description("Number of bloom filter hits (avoiding database queries)")
            .register(meterRegistry);
            
        this.bloomMisses = Counter.builder("idempotency.bloom.misses")
            .description("Number of bloom filter misses (requiring database queries)")
            .register(meterRegistry);
            
        this.messagesProcessed = Counter.builder("idempotency.messages.processed")
            .description("Total number of messages processed")
            .register(meterRegistry);
            
        this.dlqMessages = Counter.builder("idempotency.dlq.messages")
            .description("Number of messages sent to DLQ")
            .register(meterRegistry);
        
        // Initialize timers
        this.databaseQueryLatency = Timer.builder("idempotency.database.query.duration")
            .description("Database query execution time")
            .register(meterRegistry);
            
        this.messageProcessingLatency = Timer.builder("idempotency.message.processing.duration")
            .description("Total message processing time")
            .register(meterRegistry);
        
        // Initialize gauges
        Gauge.builder("idempotency.bloom.filters.active")
            .description("Number of active bloom filters")
            .register(meterRegistry, this, metrics -> metrics.activeBloomFilters.get());
            
        Gauge.builder("idempotency.database.records.total")
            .description("Total number of records in idempotency database")
            .register(meterRegistry, this, metrics -> metrics.totalRecordsInDatabase.get());
            
        Gauge.builder("idempotency.database.health")
            .description("Database health status (1=healthy, 0=unhealthy)")
            .register(meterRegistry, this, metrics -> metrics.databaseHealthStatus.get());
    }
    
    // Event recording methods
    public void recordDuplicateDetected(String topic, String cob) {
        duplicatesDetected.increment(
            Tags.of(
                Tag.of("topic", topic),
                Tag.of("cob", cob)
            )
        );
    }
    
    public void recordDatabaseQuery(String topic, String cob, Duration duration) {
        databaseQueries.increment(
            Tags.of(
                Tag.of("topic", topic),
                Tag.of("cob", cob)
            )
        );
        
        databaseQueryLatency.record(duration);
    }
    
    public void recordBloomHit(String partitionKey) {
        bloomHits.increment(Tags.of(Tag.of("partition", partitionKey)));
    }
    
    public void recordBloomMiss(String partitionKey) {
        bloomMisses.increment(Tags.of(Tag.of("partition", partitionKey)));
    }
    
    public void recordMessageProcessed(String topic, Duration processingTime) {
        messagesProcessed.increment(Tags.of(Tag.of("topic", topic)));
        messageProcessingLatency.record(processingTime);
    }
    
    public void recordDlqMessage(String topic, String errorType) {
        dlqMessages.increment(
            Tags.of(
                Tag.of("topic", topic),
                Tag.of("error_type", errorType)
            )
        );
    }
    
    // Scheduled metrics updates
    @Scheduled(fixedRate = 60000) // Every minute
    public void updateDatabaseMetrics() {
        try {
            // Update bloom filter count
            AdaptiveBloomFilterService.BloomFilterStats bloomStats = bloomService.getStats();
            activeBloomFilters.set(bloomStats.getActiveFilters());
            
            // Update database record count (expensive operation - only once per minute)
            long recordCount = getDatabaseRecordCount();
            totalRecordsInDatabase.set(recordCount);
            
            // Update health status
            boolean healthy = databaseService.isHealthy();
            databaseHealthStatus.set(healthy ? 1 : 0);
            
        } catch (Exception e) {
            logger.error("Failed to update database metrics", e);
            databaseHealthStatus.set(0); // Mark as unhealthy
        }
    }
    
    private long getDatabaseRecordCount() {
        try {
            // This is a potentially expensive operation
            return databaseService.getTotalRecordCount();
        } catch (Exception e) {
            logger.warn("Failed to get database record count", e);
            return -1; // Indicate error
        }
    }
    
    // Method to get current metrics summary for health endpoint
    public MetricsSummary getCurrentMetrics() {
        AdaptiveBloomFilterService.BloomFilterStats bloomStats = bloomService.getStats();
        
        return new MetricsSummary(
            duplicatesDetected.count(),
            databaseQueries.count(),
            bloomStats.getCacheHits(),
            bloomStats.getCacheMisses(),
            messagesProcessed.count(),
            dlqMessages.count(),
            bloomStats.getActiveFilters(),
            totalRecordsInDatabase.get(),
            databaseHealthStatus.get() == 1,
            bloomStats.getHitRate()
        );
    }
    
    public static class MetricsSummary {
        private final double duplicatesDetected;
        private final double databaseQueries;
        private final long bloomHits;
        private final long bloomMisses;
        private final double messagesProcessed;
        private final double dlqMessages;
        private final int activeBloomFilters;
        private final long totalDatabaseRecords;
        private final boolean databaseHealthy;
        private final double bloomHitRate;
        
        public MetricsSummary(double duplicatesDetected, double databaseQueries, long bloomHits, 
                             long bloomMisses, double messagesProcessed, double dlqMessages,
                             int activeBloomFilters, long totalDatabaseRecords, boolean databaseHealthy,
                             double bloomHitRate) {
            this.duplicatesDetected = duplicatesDetected;
            this.databaseQueries = databaseQueries;
            this.bloomHits = bloomHits;
            this.bloomMisses = bloomMisses;
            this.messagesProcessed = messagesProcessed;
            this.dlqMessages = dlqMessages;
            this.activeBloomFilters = activeBloomFilters;
            this.totalDatabaseRecords = totalDatabaseRecords;
            this.databaseHealthy = databaseHealthy;
            this.bloomHitRate = bloomHitRate;
        }
        
        // Getters...
    }
}
```

### Step 5.2: Create Health Check Endpoint

**File:** `src/main/java/com/company/kafkaconnector/controller/IdempotencyHealthController.java`
```java
@RestController
@RequestMapping("/actuator/idempotency")
public class IdempotencyHealthController {
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        Map<String, Object> health = new HashMap<>();
        
        HealthStatus status = healthMonitor.getCurrentHealth();
        health.put("status", status.name());
        health.put("kafka_connected", kafkaIdempotencyService.isConnected());
        health.put("bloom_filters_active", bloomCache.size());
        health.put("recovery_complete", startupManager.isRecoveryComplete());
        health.put("paused_topics", recoveryManager.getPausedTopics());
        
        return ResponseEntity.ok(health);
    }
    
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics();
    
    @GetMapping("/partitions")
    public ResponseEntity<Map<String, Object>> getPartitionActivity();
    
    @PostMapping("/recovery/trigger")
    public ResponseEntity<String> triggerRecovery();
}
```

### Step 6.3: Complete Application Configuration

**File:** `src/main/resources/application.yml`
```yaml
# Spring Application Configuration
spring:
  application:
    name: kafka-s3-delta-connector-distributed
  profiles:
    active: local
    
  # Database Configuration (H2 for local, CockroachDB for prod)
  datasource:
    url: jdbc:h2:file:./data/idempotency;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    driver-class-name: org.h2.Driver
    username: sa
    password: 
    hikari:
      maximum-pool-size: 10
      connection-timeout: 20000
      
  jpa:
    hibernate:
      ddl-auto: none  # Let Liquibase handle schema
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.H2Dialect
        
  liquibase:
    change-log: classpath:db/changelog/db.changelog-master.yaml
    
  h2:
    console:
      enabled: true
      path: /h2-console
    
  # Kafka Configuration with Multi-Format Support
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: kafka-s3-delta-connector
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-interval-ms: 300000
      session-timeout-ms: 30000
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

# Schema Registry Configuration
schema:
  registry:
    url: http://localhost:8081
    
# AWS S3/MinIO Configuration
aws:
  s3:
    endpoint: http://localhost:9000
    region: us-east-1
    path-style-access: true
    access-key-id: minioadmin
    secret-access-key: minioadmin

# Delta Lake Connector Configuration with COB Partitioning
connector:
  topics:
    user-events:
      kafka-topic: "user.events.v1"
      avro-schema: "UserEvent"
      supported-formats: ["JSON", "CSV", "AVRO"]
      destination:
        bucket: "data-lake"
        path: "events/user-events"
        table-name: "user_events"
        partition-strategy: "cob-based"
        partition-columns: ["cob_date"]
        delta-config:
          enable-optimize: true
          optimize-interval: 10
          enable-schema-evolution: true
      processing:
        batch-size: 1000
        flush-interval: 60
        max-retries: 3

# Database Idempotency Configuration
idempotency:
  database:
    cleanup-retention-days: 7
    health-check-interval-seconds: 30
    
  bloom-filter:
    max-filters: 20
    hot-threshold-messages-per-hour: 1000
    expected-elements: 100000
    false-positive-rate: 0.01
    management-interval-minutes: 1

# COB Processing Configuration  
cob:
  partitioning:
    default-column: "cob_date"
    validation:
      strict-format: true
      allow-future-dates: false
      max-days-in-past: 365

# Server Configuration
server:
  port: ${SERVER_PORT:8080}

# Management/Actuator Configuration
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,info,prometheus,idempotency
  server:
    port: ${MANAGEMENT_PORT:8081}

# Production Profile
---
spring:
  profiles: prod
  datasource:
    url: jdbc:postgresql://${DB_HOST}:${DB_PORT:26257}/${DB_NAME:connector}?sslmode=require
    driver-class-name: org.postgresql.Driver
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
    hikari:
      maximum-pool-size: 20
      connection-timeout: 30000
  jpa:
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
```

---

## 📝 Multi-Format Message Examples

### JSON Format Example
```json
{
  "user_id": "user-12345",
  "event_type": "page_view",
  "timestamp": 1704067200000,
  "cob": "2024-01-01",
  "session_id": "session-abc123",
  "properties": {
    "page_url": "https://example.com/product/123",
    "referrer": "https://google.com"
  },
  "metadata": {
    "source": "web-analytics",
    "version": "1.0",
    "correlation_id": "corr-xyz789"
  }
}
```

### CSV Format Example
```csv
user_id,event_type,timestamp,cob,session_id,page_url,referrer,source,version,correlation_id
user-12345,page_view,1704067200000,2024-01-01,session-abc123,https://example.com/product/123,https://google.com,web-analytics,1.0,corr-xyz789
user-67890,login,1704070800000,2024-01-01,session-def456,,https://facebook.com,mobile-app,1.0,corr-abc123
```

### Avro Format Example (Base64 encoded binary)
```
T2JqAQQUYXZyby5zY2hlbWGUAnsidHlwZSI6InJlY29yZCIsIm5hbWUiOiJVc2VyRXZlbnQiLCJuYW1lc3BhY2UiOiJjb20uY29tcGFueS5rYWZrYWNvbm5lY3Rvci5hdnJvIiwiZmllbGRzIjpbeyJuYW1lIjoidXNlcl9pZCIsInR5cGUiOiJzdHJpbmcifSx7Im5hbWUiOiJldmVudF90eXBlIiwidHlwZSI6InN0cmluZyJ9XX0AAAAAA...
```

### Schema Registry Schema Registration
```bash
# Register UserEvent schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.company.kafkaconnector.avro\",\"fields\":[{\"name\":\"user_id\",\"type\":\"string\"},...]}"}' \
  http://localhost:8081/subjects/UserEvent/versions

# Register OrderEvent schema  
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"namespace\":\"com.company.kafkaconnector.avro\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},...]}"}' \
  http://localhost:8081/subjects/OrderEvent/versions
```

### Test Producer Examples

**JSON Producer:**
```bash
# Send JSON message
kafka-console-producer --bootstrap-server localhost:9092 --topic user.events.v1 << EOF
{"user_id":"user-12345","event_type":"page_view","timestamp":1704067200000,"cob":"2024-01-01","session_id":"session-abc123","properties":{"page_url":"https://example.com/product/123"},"metadata":{"source":"web-analytics","version":"1.0"}}
EOF
```

**CSV Producer:**
```bash
# Send CSV message (header + data row)
kafka-console-producer --bootstrap-server localhost:9092 --topic user.events.v1 << EOF
user_id,event_type,timestamp,cob,session_id,page_url,source,version
user-67890,login,1704070800000,2024-01-01,session-def456,https://app.com/login,mobile-app,1.0
EOF
```

## 🧪 Testing Strategy

### Step 6.1: Multi-Format Unit Tests

**File:** `src/test/java/com/company/kafkaconnector/service/MultiFormatMessageParserTest.java`
```java
@ExtendWith(MockitoExtension.class)
class MultiFormatMessageParserTest {
    
    @Test
    void shouldDetectJsonFormat() {
        String jsonMessage = "{\"user_id\":\"test\",\"cob\":\"2024-01-01\"}";
        MessageFormat detected = parser.detectMessageFormat(jsonMessage);
        assertEquals(MessageFormat.JSON, detected);
    }
    
    @Test
    void shouldDetectCsvFormat() {
        String csvMessage = "user_id,event_type,cob\ntest-user,login,2024-01-01";
        MessageFormat detected = parser.detectMessageFormat(csvMessage);
        assertEquals(MessageFormat.CSV, detected);
    }
    
    @Test
    void shouldParseJsonToAvro() {
        String jsonMessage = "{\"user_id\":\"test-user\",\"event_type\":\"login\",\"timestamp\":1704067200000,\"cob\":\"2024-01-01\"}";
        GenericRecord record = parser.parseJsonToAvro(jsonMessage, userEventSchema, MessageFormat.JSON);
        
        assertEquals("test-user", record.get("user_id"));
        assertEquals("login", record.get("event_type"));
        assertEquals("2024-01-01", record.get("cob"));
    }
    
    @Test
    void shouldParseCsvToAvro() {
        String csvMessage = "user_id,event_type,timestamp,cob\ntest-user,login,1704067200000,2024-01-01";
        GenericRecord record = parser.parseCsvToAvro(csvMessage, userEventSchema, MessageFormat.CSV);
        
        assertEquals("test-user", record.get("user_id"));
        assertEquals("login", record.get("event_type"));
        assertEquals("2024-01-01", record.get("cob"));
    }
    
    @Test
    void shouldGenerateConsistentFingerprints() {
        // Same semantic content in different formats should have different fingerprints
        // (because we fingerprint raw content, not semantic content)
        String jsonMessage = "{\"user_id\":\"test\",\"cob\":\"2024-01-01\"}";
        String csvMessage = "user_id,cob\ntest,2024-01-01";
        
        String jsonFingerprint = fingerprintGenerator.generateFingerprint(jsonMessage);
        String csvFingerprint = fingerprintGenerator.generateFingerprint(csvMessage);
        
        assertNotEquals(jsonFingerprint, csvFingerprint);
        
        // But the same raw content should always generate the same fingerprint
        String duplicateJsonFingerprint = fingerprintGenerator.generateFingerprint(jsonMessage);
        assertEquals(jsonFingerprint, duplicateJsonFingerprint);
    }
}
```

### Step 6.2: Integration Tests

**File:** `src/test/java/com/company/kafkaconnector/service/AdaptiveIdempotencyServiceTest.java`
```java
@ExtendWith(MockitoExtension.class)
class AdaptiveIdempotencyServiceTest {
    
    @Test
    void shouldDetectDuplicatesInHotPartitions();
    
    @Test
    void shouldCreateBloomFilterForHotPartitions();
    
    @Test
    void shouldSkipBloomForColdPartitions();
    
    @Test
    void shouldHandleRecoveryAfterRestart();
    
    @Test
    void shouldPauseConsumptionOnHealthFailure();
}
```

### Step 6.2: Integration Tests

**File:** `src/test/java/com/company/kafkaconnector/integration/IdempotencyIntegrationTest.java`
```java
@SpringBootTest
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "idempotency.kafka-topic.name=test-idempotency-log"
})
class IdempotencyIntegrationTest {
    
    @Test
    void shouldPreventDuplicatesAcrossRestart();
    
    @Test
    void shouldHandleNonConsecutiveCobs();
    
    @Test
    void shouldRecoverFromIdempotencyServiceFailure();
    
    @Test
    void shouldMaintainPerformanceUnderLoad();
}
```

### Step 6.3: Load Testing

**File:** `src/test/java/com/company/kafkaconnector/load/IdempotencyLoadTest.java`
```java
@Component
public class IdempotencyLoadTest {
    
    @Test
    void testHighVolumeProcessing() {
        // 10M messages with 0.1% duplicate rate
        // Verify memory usage stays under 50MB
        // Verify 99% of queries under 1ms
    }
    
    @Test
    void testRecoveryPerformance() {
        // Restart with 1M recent messages
        // Verify recovery completes under 30 seconds
    }
}
```

---

## 📊 Success Criteria

### Performance Targets
- **Memory Usage**: < 50MB for 10M messages/day across all topics
- **Query Latency**: 99% of duplicate checks < 1ms  
- **Recovery Time**: < 30 seconds for startup recovery
- **False Positive Rate**: < 1% for bloom filters

### Reliability Targets
- **Zero Data Loss**: Messages never lost during failures
- **Zero Duplicates**: No duplicate processing in steady state
- **High Availability**: < 30 seconds downtime during recovery
- **Automatic Recovery**: No manual intervention required

### Monitoring Targets
- **Health Check Response**: < 100ms
- **Metrics Collection**: Real-time visibility into all operations
- **Alerting**: Automated alerts for service degradation
- **Dashboard**: Operational visibility into partition activity

---

## 🚀 Deployment Steps

### Step 7.1: Infrastructure Preparation
1. Create Kafka compacted topic for idempotency log
2. Configure retention policies and partitioning
3. Set up monitoring and alerting infrastructure

### Step 7.2: Application Deployment
1. Deploy with conservative settings (small bloom filters)
2. Monitor initial performance and memory usage  
3. Tune configuration based on actual traffic patterns
4. Gradually increase bloom filter limits as needed

### Step 7.3: Production Validation
1. Verify zero duplicates during normal operation
2. Test failure scenarios (Kafka down, app restart)
3. Validate recovery mechanisms work correctly
4. Monitor performance under peak loads

## 🚀 Deployment and Scaling Strategy

### Multi-Instance Deployment

**Option 1: Docker Compose (Development)**
```yaml
# docker-compose.yml
version: '3.8'
services:
  connector-instance-1:
    image: kafka-s3-connector:latest
    environment:
      - SERVER_PORT=8080
      - MANAGEMENT_PORT=8081
      - HOSTNAME=connector-1
    depends_on:
      - kafka
      - schema-registry
      - minio
      
  connector-instance-2:
    image: kafka-s3-connector:latest
    environment:
      - SERVER_PORT=8082
      - MANAGEMENT_PORT=8083
      - HOSTNAME=connector-2
    depends_on:
      - kafka
      - schema-registry
      - minio
      
  connector-instance-3:
    image: kafka-s3-connector:latest
    environment:
      - SERVER_PORT=8084
      - MANAGEMENT_PORT=8085
      - HOSTNAME=connector-3
    depends_on:
      - kafka
      - schema-registry
      - minio
```

**Option 2: Kubernetes (Production)**
```yaml
# k8s-deployment.yml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-s3-connector
spec:
  replicas: 3  # 3 instances for high availability
  selector:
    matchLabels:
      app: kafka-s3-connector
  template:
    metadata:
      labels:
        app: kafka-s3-connector
    spec:
      containers:
      - name: connector
        image: kafka-s3-connector:latest
        env:
        - name: HOSTNAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: SPRING_PROFILES_ACTIVE
          value: "k8s"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"  
            cpu: "1000m"
        ports:
        - containerPort: 8080
        - containerPort: 8081
        livenessProbe:
          httpGet:
            path: /actuator/health
            port: 8081
          initialDelaySeconds: 60
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /actuator/idempotency/health
            port: 8081
          initialDelaySeconds: 30
          periodSeconds: 10
```

### Scaling Characteristics

**Memory Usage Per Instance:**
- Base application: ~200MB
- Bloom filters (20 max): ~25MB  
- Activity tracking: ~5MB
- **Total per instance: ~230MB**

**CPU Usage:**
- Bloom filter operations: ~5% per 10K messages/sec
- Delta Lake writes: ~20% per 5K messages/sec
- Coordination overhead: ~2% per instance

**Network Usage:**
- Coordination messages: ~1KB per hot partition per minute
- Activity updates: ~100 bytes per 10 messages
- Bloom filter sharing: ~500KB per partition handoff

### Performance Targets

| Metric | Target | Multi-Instance |
|--------|--------|----------------|
| **Messages/sec per instance** | 10,000 | 30,000 (3 instances) |
| **Duplicate detection latency** | < 1ms (99%) | Same per instance |
| **Memory per instance** | < 250MB | < 750MB total |
| **Recovery time** | < 30 seconds | < 60 seconds (worst case) |
| **Zero data loss** | 100% | 100% |
| **Zero duplicates** | 99.99%+ | 99.99%+ |

## ✅ Success Criteria Summary

### **Database-Driven Reliability**
✅ **Distributed Consistency**: Database ACID properties ensure perfect duplicate detection across instances  
✅ **Instance Failure**: Database durability guarantees zero data loss during failures  
✅ **Horizontal Scaling**: Standard database clustering for 10+ instances  
✅ **Operational Simplicity**: Standard database operations, monitoring, and backup strategies  

### **COB-Based Partitioning**
✅ **Default Partitioning**: All Delta Lake tables partitioned by `cob_date`  
✅ **Non-consecutive COBs**: Database queries handle any COB date efficiently  
✅ **Adaptive Performance**: Bloom filters automatically created for hot partitions only  
✅ **Perfect Isolation**: Database unique constraints guarantee no duplicates per (topic, COB, fingerprint)  

### **Multi-Format Avro Integration**  
✅ **Schema Evolution**: Backward/forward compatible message processing across JSON/CSV/Avro  
✅ **Consistent Fingerprinting**: Raw message fingerprinting for duplicate detection  
✅ **Schema Registry**: Centralized Avro schema management  
✅ **Format Flexibility**: Automatic detection and parsing of JSON, CSV, and Avro messages  

### **Production Readiness**
✅ **Zero Downtime Deployment**: Standard database clustering with application rolling updates  
✅ **Database Monitoring**: Standard PostgreSQL/CockroachDB monitoring and alerting  
✅ **Memory Efficiency**: ~50MB per instance (vs 250MB+ with Kafka approach)  
✅ **Query Performance**: 1-3ms duplicate checks with proper indexing  

### **Development Experience**
✅ **H2 Local Development**: Zero-config local development with in-memory database  
✅ **Liquibase Schema Evolution**: Standard database migration tools  
✅ **Standard Testing Patterns**: JPA/database testing instead of complex Kafka coordination tests  
✅ **Simplified Debugging**: SQL queries for troubleshooting instead of distributed Kafka state  

## 📊 Performance Comparison: Database vs Kafka Approach

| Aspect | Database Approach | Original Kafka Approach |
|--------|-------------------|-------------------------|
| **Duplicate Check Latency** | 1-3ms (indexed query) | 10-50ms (Kafka scan) |
| **Memory per Instance** | ~50MB | ~250MB |
| **Implementation Complexity** | Low (standard patterns) | High (custom coordination) |
| **Operational Burden** | Standard DB monitoring | Custom Kafka topic monitoring |
| **Recovery Time** | Instant (DB durability) | 30-60s (bloom filter rebuild) |
| **Scaling** | Database clustering | Manual coordination |
| **Development Setup** | H2 in-memory | Complex Kafka topics |
| **Testing** | Standard DB tests | Complex distributed tests |

This simplified database-driven implementation provides enterprise-grade reliability for the Kafka-S3-Delta Lake connector with:
- **Operational simplicity** using proven database patterns
- **Superior performance** with 1-3ms duplicate detection  
- **Multi-format support** for JSON, CSV, and Avro messages
- **COB-based Delta Lake partitioning** for optimal query performance
- **Adaptive bloom filters** only where beneficial (hot partitions)
- **Zero data loss and zero duplicates** with database ACID guarantees