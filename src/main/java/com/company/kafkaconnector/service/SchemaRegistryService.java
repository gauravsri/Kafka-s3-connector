package com.company.kafkaconnector.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for managing Schema Registry interactions and caching Avro schemas.
 */
@Service
public class SchemaRegistryService {
    
    private static final Logger logger = LoggerFactory.getLogger(SchemaRegistryService.class);
    
    @Value("${schema.registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;
    
    private SchemaRegistryClient schemaRegistryClient;
    private final ConcurrentHashMap<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> latestVersionCache = new ConcurrentHashMap<>();
    
    @PostConstruct
    public void initialize() {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        logger.info("Initialized Schema Registry client with URL: {}", schemaRegistryUrl);
    }
    
    /**
     * Get the latest schema for a subject
     */
    public Schema getLatestSchema(String subject) {
        return schemaCache.computeIfAbsent(subject, this::fetchLatestSchema);
    }
    
    /**
     * Get a specific version of a schema for a subject
     */
    public Schema getSchemaByVersion(String subject, int version) {
        String cacheKey = subject + ":" + version;
        return schemaCache.computeIfAbsent(cacheKey, key -> fetchSchemaByVersion(subject, version));
    }
    
    /**
     * Get schema by ID (used for Avro deserialization)
     */
    public Schema getSchemaById(int schemaId) {
        String cacheKey = "id:" + schemaId;
        return schemaCache.computeIfAbsent(cacheKey, key -> fetchSchemaById(schemaId));
    }
    
    /**
     * Check if a schema is compatible with the latest version
     */
    public boolean isCompatible(String subject, Schema schema) {
        try {
            return schemaRegistryClient.testCompatibility(subject, schema);
        } catch (IOException | RestClientException e) {
            logger.warn("Failed to check compatibility for subject: {}", subject, e);
            return false;
        }
    }
    
    /**
     * Register a new schema version
     */
    public int registerSchema(String subject, Schema schema) {
        try {
            int id = schemaRegistryClient.register(subject, schema);
            // Update cache
            schemaCache.put(subject, schema);
            logger.info("Registered new schema for subject: {} with ID: {}", subject, id);
            return id;
        } catch (IOException | RestClientException e) {
            logger.error("Failed to register schema for subject: {}", subject, e);
            throw new RuntimeException("Schema registration failed", e);
        }
    }
    
    /**
     * Get all subjects
     */
    public java.util.Collection<String> getAllSubjects() {
        try {
            return schemaRegistryClient.getAllSubjects();
        } catch (IOException | RestClientException e) {
            logger.error("Failed to fetch all subjects", e);
            throw new RuntimeException("Failed to fetch subjects", e);
        }
    }
    
    /**
     * Clear schema cache (for testing or manual refresh)
     */
    public void clearCache() {
        schemaCache.clear();
        latestVersionCache.clear();
        logger.info("Cleared schema cache");
    }
    
    private Schema fetchLatestSchema(String subject) {
        try {
            String schemaString = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema().toString();
            Schema.Parser parser = new Schema.Parser();
            Schema avroSchema = parser.parse(schemaString);
            logger.debug("Fetched latest schema for subject: {}", subject);
            return avroSchema;
        } catch (IOException | RestClientException e) {
            logger.error("Failed to fetch latest schema for subject: {}", subject, e);
            throw new RuntimeException("Failed to fetch schema", e);
        }
    }
    
    private Schema fetchSchemaByVersion(String subject, int version) {
        try {
            String schemaString = schemaRegistryClient.getSchemaMetadata(subject, version).getSchema().toString();
            Schema.Parser parser = new Schema.Parser();
            Schema avroSchema = parser.parse(schemaString);
            logger.debug("Fetched schema version {} for subject: {}", version, subject);
            return avroSchema;
        } catch (IOException | RestClientException e) {
            logger.error("Failed to fetch schema version {} for subject: {}", version, subject, e);
            throw new RuntimeException("Failed to fetch schema", e);
        }
    }
    
    private Schema fetchSchemaById(int schemaId) {
        try {
            String schemaString = schemaRegistryClient.getSchemaById(schemaId).toString();
            Schema.Parser parser = new Schema.Parser();
            Schema avroSchema = parser.parse(schemaString);
            logger.debug("Fetched schema by ID: {}", schemaId);
            return avroSchema;
        } catch (IOException | RestClientException e) {
            logger.error("Failed to fetch schema by ID: {}", schemaId, e);
            throw new RuntimeException("Failed to fetch schema", e);
        }
    }
    
    /**
     * Health check for Schema Registry connectivity
     */
    public boolean isHealthy() {
        try {
            schemaRegistryClient.getAllSubjects();
            return true;
        } catch (Exception e) {
            logger.warn("Schema Registry health check failed", e);
            return false;
        }
    }
    
    /**
     * Get cache statistics for monitoring
     */
    public SchemaRegistryStats getStats() {
        return new SchemaRegistryStats(
            schemaCache.size(),
            latestVersionCache.size(),
            schemaRegistryUrl,
            isHealthy()
        );
    }
    
    public static class SchemaRegistryStats {
        private final int cachedSchemas;
        private final int cachedVersions;
        private final String registryUrl;
        private final boolean healthy;
        
        public SchemaRegistryStats(int cachedSchemas, int cachedVersions, String registryUrl, boolean healthy) {
            this.cachedSchemas = cachedSchemas;
            this.cachedVersions = cachedVersions;
            this.registryUrl = registryUrl;
            this.healthy = healthy;
        }
        
        public int getCachedSchemas() { return cachedSchemas; }
        public int getCachedVersions() { return cachedVersions; }
        public String getRegistryUrl() { return registryUrl; }
        public boolean isHealthy() { return healthy; }
    }
}