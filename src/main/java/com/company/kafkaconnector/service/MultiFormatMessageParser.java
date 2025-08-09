package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.MessageParsingException;
import com.company.kafkaconnector.model.TopicConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class MultiFormatMessageParser {
    
    private static final Logger logger = LoggerFactory.getLogger(MultiFormatMessageParser.class);
    
    private final ObjectMapper jsonMapper;
    private final SchemaRegistryService schemaRegistryService;
    private final CSVParser csvParser;
    
    public MultiFormatMessageParser(SchemaRegistryService schemaRegistryService) {
        this.jsonMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.schemaRegistryService = schemaRegistryService;
        this.csvParser = new CSVParserBuilder()
            .withSeparator(',')
            .withQuoteChar('"')
            .withEscapeChar('\\')
            .withIgnoreLeadingWhiteSpace(true)
            .build();
    }
    
    public GenericRecord parseMessage(String rawMessage, String topicName, TopicConfig topicConfig) {
        try {
            MessageFormat detectedFormat = detectMessageFormat(rawMessage);
            logger.debug("Detected format: {} for topic: {}", detectedFormat, topicName);
            
            // Load schema from Schema Registry or use fallback
            Schema schema = getSchemaForTopic(topicConfig, topicName);
            
            switch (detectedFormat) {
                case JSON:
                    return parseJsonToAvro(rawMessage, schema);
                case CSV:
                    return parseCsvToAvro(rawMessage, schema);
                case AVRO:
                    return parseAvroToAvro(rawMessage, schema);
                default:
                    throw new MessageParsingException("Unsupported message format for topic: " + topicName);
            }
        } catch (Exception e) {
            throw new MessageParsingException("Failed to parse message for topic: " + topicName, e);
        }
    }
    
    private MessageFormat detectMessageFormat(String rawMessage) {
        String trimmed = rawMessage.trim();
        
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
            (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            return MessageFormat.JSON;
        }
        
        if (trimmed.contains(",") && !trimmed.contains("{") && !trimmed.contains("[")) {
            return MessageFormat.CSV;
        }
        
        return MessageFormat.JSON; // Default fallback
    }
    
    private GenericRecord parseJsonToAvro(String jsonMessage, Schema schema) {
        try {
            JsonNode jsonNode = jsonMapper.readTree(jsonMessage);
            GenericRecord record = new GenericData.Record(schema);
            
            // Map all fields from JSON to Avro based on schema
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                JsonNode fieldValue = jsonNode.get(fieldName);
                
                if (fieldValue != null && !fieldValue.isNull()) {
                    Object convertedValue = convertJsonValueToAvro(fieldValue, field.schema());
                    record.put(fieldName, convertedValue);
                } else if (field.hasDefaultValue()) {
                    record.put(fieldName, field.defaultVal());
                }
            }
            
            // Ensure COB is present
            ensureCobField(record, jsonNode);
            
            return record;
        } catch (Exception e) {
            throw new MessageParsingException("Failed to parse JSON message", e);
        }
    }
    
    private GenericRecord parseCsvToAvro(String csvMessage, Schema schema) {
        try {
            GenericRecord record = new GenericData.Record(schema);
            
            // Parse CSV using OpenCSV
            StringReader stringReader = new StringReader(csvMessage);
            CSVReader csvReader = new CSVReaderBuilder(stringReader)
                .withCSVParser(csvParser)
                .build();
            
            String[] csvFields = csvReader.readNext();
            if (csvFields == null || csvFields.length == 0) {
                throw new MessageParsingException("Empty CSV message");
            }
            
            // Map CSV fields to Avro schema fields by position
            List<Schema.Field> schemaFields = schema.getFields();
            for (int i = 0; i < Math.min(csvFields.length, schemaFields.size()); i++) {
                Schema.Field field = schemaFields.get(i);
                String csvValue = csvFields[i].trim();
                
                if (!csvValue.isEmpty()) {
                    Object convertedValue = convertCsvValueToAvro(csvValue, field.schema());
                    record.put(field.name(), convertedValue);
                } else if (field.hasDefaultValue()) {
                    record.put(field.name(), field.defaultVal());
                }
            }
            
            // Ensure COB field is present
            if (csvFields.length > 0 && record.get("cob") == null) {
                record.put("cob", csvFields[0].trim()); // Assume first field is COB
            }
            
            csvReader.close();
            return record;
            
        } catch (Exception e) {
            throw new MessageParsingException("Failed to parse CSV message", e);
        }
    }
    
    private GenericRecord parseAvroToAvro(String avroMessage, Schema schema) {
        try {
            // Handle base64 encoded Avro data from Kafka
            byte[] avroBytes;
            try {
                avroBytes = Base64.getDecoder().decode(avroMessage);
            } catch (IllegalArgumentException e) {
                // If not base64, assume it's JSON format Avro
                return parseJsonToAvro(avroMessage, schema);
            }
            
            // Extract schema ID from magic byte (Confluent format)
            if (avroBytes.length < 5) {
                throw new MessageParsingException("Invalid Avro message format");
            }
            
            ByteBuffer buffer = ByteBuffer.wrap(avroBytes);
            byte magicByte = buffer.get(); // Should be 0x0
            int schemaId = buffer.getInt();
            
            if (magicByte != 0) {
                throw new MessageParsingException("Invalid Avro magic byte");
            }
            
            // Get schema from Schema Registry
            Schema writerSchema = schemaRegistryService.getSchemaById(schemaId);
            
            // Deserialize Avro binary data
            byte[] payload = new byte[avroBytes.length - 5];
            buffer.get(payload);
            
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
            
            return reader.read(null, decoder);
            
        } catch (Exception e) {
            throw new MessageParsingException("Failed to parse Avro message", e);
        }
    }
    
    /**
     * Get schema for topic from Schema Registry or use fallback
     */
    private Schema getSchemaForTopic(TopicConfig topicConfig, String topicName) {
        try {
            // Try to get schema from Schema Registry using topic config
            if (topicConfig.getSchemaFile() != null) {
                // Extract subject name from schema file path (e.g., schemas/user-events-schema.json -> user-events)
                String schemaFileName = topicConfig.getSchemaFile();
                String subject = extractSubjectFromSchemaFile(schemaFileName) + "-value";
                return schemaRegistryService.getLatestSchema(subject);
            }
            
            // Fallback: try topic name as subject
            String subject = topicName + "-value";
            return schemaRegistryService.getLatestSchema(subject);
            
        } catch (Exception e) {
            logger.warn("Failed to load schema from Schema Registry for topic: {}, using fallback schema", topicName, e);
            return getFallbackSchema();
        }
    }
    
    /**
     * Fallback schema when Schema Registry is unavailable
     */
    private Schema getFallbackSchema() {
        String schemaJson = "{"
            + "\"type\": \"record\","
            + "\"name\": \"GenericMessage\","
            + "\"fields\": ["
            + "  {\"name\": \"cob\", \"type\": \"string\"},"
            + "  {\"name\": \"user_id\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "  {\"name\": \"event_type\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "  {\"name\": \"timestamp\", \"type\": [\"null\", \"long\"], \"default\": null},"
            + "  {\"name\": \"data\", \"type\": [\"null\", \"string\"], \"default\": null},"
            + "  {\"name\": \"properties\", \"type\": [{\"type\": \"map\", \"values\": \"string\"}, \"null\"], \"default\": {}}"
            + "]"
            + "}";
        
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaJson);
    }
    
    /**
     * Convert JSON value to appropriate Avro type
     */
    private Object convertJsonValueToAvro(JsonNode jsonValue, Schema fieldSchema) {
        // Handle union types (nullable fields)
        if (fieldSchema.getType() == Schema.Type.UNION) {
            if (jsonValue.isNull()) {
                return null;
            }
            // Find non-null type in union
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
            case FLOAT:
                return (float) jsonValue.asDouble();
            case BOOLEAN:
                return jsonValue.asBoolean();
            case BYTES:
                return Base64.getDecoder().decode(jsonValue.asText());
            case ARRAY:
                List<Object> arrayList = new java.util.ArrayList<>();
                for (JsonNode element : jsonValue) {
                    arrayList.add(convertJsonValueToAvro(element, fieldSchema.getElementType()));
                }
                return arrayList;
            case MAP:
                Map<String, Object> mapValue = new HashMap<>();
                jsonValue.fields().forEachRemaining(entry -> {
                    Object convertedValue = convertJsonValueToAvro(entry.getValue(), fieldSchema.getValueType());
                    mapValue.put(entry.getKey(), convertedValue);
                });
                return mapValue;
            case RECORD:
                GenericRecord recordValue = new GenericData.Record(fieldSchema);
                for (Schema.Field field : fieldSchema.getFields()) {
                    JsonNode fieldValue = jsonValue.get(field.name());
                    if (fieldValue != null) {
                        recordValue.put(field.name(), convertJsonValueToAvro(fieldValue, field.schema()));
                    }
                }
                return recordValue;
            case ENUM:
                return new GenericData.EnumSymbol(fieldSchema, jsonValue.asText());
            default:
                return jsonValue.asText();
        }
    }
    
    /**
     * Convert CSV value to appropriate Avro type
     */
    private Object convertCsvValueToAvro(String csvValue, Schema fieldSchema) {
        // Handle union types (nullable fields)
        if (fieldSchema.getType() == Schema.Type.UNION) {
            if (csvValue.isEmpty() || "null".equalsIgnoreCase(csvValue)) {
                return null;
            }
            // Find non-null type in union
            for (Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() != Schema.Type.NULL) {
                    return convertCsvValueToAvro(csvValue, unionType);
                }
            }
        }
        
        switch (fieldSchema.getType()) {
            case STRING:
                return csvValue;
            case INT:
                return Integer.parseInt(csvValue);
            case LONG:
                if ("timestamp-millis".equals(fieldSchema.getProp("logicalType"))) {
                    return parseTimestampToMillis(csvValue);
                }
                return Long.parseLong(csvValue);
            case DOUBLE:
                return Double.parseDouble(csvValue);
            case FLOAT:
                return Float.parseFloat(csvValue);
            case BOOLEAN:
                return Boolean.parseBoolean(csvValue);
            case BYTES:
                return Base64.getDecoder().decode(csvValue);
            case ENUM:
                return new GenericData.EnumSymbol(fieldSchema, csvValue);
            default:
                return csvValue;
        }
    }
    
    /**
     * Parse timestamp string to milliseconds
     */
    private long parseTimestampToMillis(String timestampStr) {
        try {
            // Try parsing as ISO instant
            return java.time.Instant.parse(timestampStr).toEpochMilli();
        } catch (Exception e1) {
            try {
                // Try parsing as epoch millis
                return Long.parseLong(timestampStr);
            } catch (Exception e2) {
                try {
                    // Try parsing as local date time
                    LocalDateTime ldt = LocalDateTime.parse(timestampStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                    return ldt.atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
                } catch (Exception e3) {
                    logger.warn("Failed to parse timestamp: {}, using current time", timestampStr);
                    return System.currentTimeMillis();
                }
            }
        }
    }
    
    /**
     * Ensure COB field is present in the record
     */
    private void ensureCobField(GenericRecord record, JsonNode jsonNode) {
        if (record.get("cob") == null) {
            // Try to extract COB from JSON
            if (jsonNode.has("cob")) {
                record.put("cob", jsonNode.get("cob").asText());
            } else if (jsonNode.has("cob_date")) {
                record.put("cob", jsonNode.get("cob_date").asText());
            } else {
                // Default to today's date
                record.put("cob", LocalDate.now().toString());
                logger.debug("COB field not found in message, using current date");
            }
        }
    }
    
    /**
     * Extract subject name from schema file path
     */
    private String extractSubjectFromSchemaFile(String schemaFilePath) {
        // Extract filename from path: schemas/user-events-schema.json -> user-events-schema.json
        String fileName = schemaFilePath.substring(schemaFilePath.lastIndexOf('/') + 1);
        
        // Remove extension: user-events-schema.json -> user-events-schema
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > 0) {
            fileName = fileName.substring(0, dotIndex);
        }
        
        // Remove -schema suffix if present: user-events-schema -> user-events
        if (fileName.endsWith("-schema")) {
            fileName = fileName.substring(0, fileName.length() - "-schema".length());
        }
        
        return fileName;
    }
    
    private enum MessageFormat {
        JSON, CSV, AVRO
    }
}