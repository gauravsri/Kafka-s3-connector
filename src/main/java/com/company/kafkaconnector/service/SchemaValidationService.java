package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.NonRetriableException;
import com.company.kafkaconnector.exception.SchemaValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class SchemaValidationService {
    
    private static final Logger logger = LoggerFactory.getLogger(SchemaValidationService.class);
    
    private final JsonSchemaFactory schemaFactory;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, JsonSchema> schemaCache = new ConcurrentHashMap<>();

    public SchemaValidationService() {
        this.schemaFactory = JsonSchemaFactory.byDefault();
        this.objectMapper = new ObjectMapper();
    }

    public boolean validateMessage(String schemaFile, String jsonMessage) {
        try {
            JsonSchema schema = getOrLoadSchema(schemaFile);
            JsonNode messageNode = objectMapper.readTree(jsonMessage);
            
            ProcessingReport report = schema.validate(messageNode);
            
            if (!report.isSuccess()) {
                logger.warn("Schema validation failed for schema: {}. Report: {}", schemaFile, report);
                // Schema validation failures are business logic errors, not malformed data
                throw new SchemaValidationException(
                    "Schema validation failed", 
                    schemaFile, 
                    jsonMessage, 
                    new RuntimeException(report.toString())
                );
            }
            
            logger.debug("Schema validation successful for schema: {}", schemaFile);
            return true;
            
        } catch (IOException e) {
            logger.error("Error parsing JSON message for schema validation: {}", e.getMessage());
            // JSON parsing errors are malformed data - cannot be retried
            throw NonRetriableException.malformedMessage("Invalid JSON format for schema " + schemaFile, e);
        } catch (ProcessingException e) {
            logger.error("Error validating message against schema {}: {}", schemaFile, e.getMessage());
            // Processing exceptions are schema-related issues
            throw new SchemaValidationException(
                "Schema validation processing error", 
                schemaFile, 
                jsonMessage, 
                e
            );
        } catch (SchemaValidationException e) {
            // Re-throw our own exception type
            throw e;
        }
    }

    private JsonSchema getOrLoadSchema(String schemaFile) {
        return schemaCache.computeIfAbsent(schemaFile, this::loadSchema);
    }

    private JsonSchema loadSchema(String schemaFile) {
        try {
            logger.info("Loading schema from file: {}", schemaFile);
            
            ClassPathResource resource = new ClassPathResource(schemaFile);
            
            if (!resource.exists()) {
                logger.warn("Schema file not found: {}. Creating a permissive default schema.", schemaFile);
                return createDefaultSchema();
            }
            
            try (InputStream inputStream = resource.getInputStream()) {
                JsonNode schemaNode = objectMapper.readTree(inputStream);
                return schemaFactory.getJsonSchema(schemaNode);
            }
            
        } catch (IOException e) {
            logger.error("Error loading schema file {}: {}", schemaFile, e.getMessage());
            throw new SchemaValidationException(
                "Failed to load schema file: " + schemaFile, 
                schemaFile, 
                null, 
                e
            );
        } catch (ProcessingException e) {
            logger.error("Error processing schema file {}: {}", schemaFile, e.getMessage());
            throw new SchemaValidationException(
                "Invalid schema format: " + schemaFile, 
                schemaFile, 
                null, 
                e
            );
        }
    }

    private JsonSchema createDefaultSchema() {
        try {
            // Create a permissive schema that accepts any JSON object
            String defaultSchemaJson = "{" +
                    "\"$schema\": \"http://json-schema.org/draft-04/schema#\"," +
                    "\"type\": \"object\"," +
                    "\"additionalProperties\": true" +
                    "}";
            
            JsonNode schemaNode = objectMapper.readTree(defaultSchemaJson);
            return schemaFactory.getJsonSchema(schemaNode);
            
        } catch (IOException | ProcessingException e) {
            throw new RuntimeException("Failed to create default schema", e);
        }
    }

    public void reloadSchema(String schemaFile) {
        logger.info("Reloading schema: {}", schemaFile);
        schemaCache.remove(schemaFile);
    }

    public void clearSchemaCache() {
        logger.info("Clearing all cached schemas");
        schemaCache.clear();
    }

    public int getCachedSchemaCount() {
        return schemaCache.size();
    }

    public boolean isSchemaCached(String schemaPath) {
        return schemaCache.containsKey(schemaPath);
    }
}