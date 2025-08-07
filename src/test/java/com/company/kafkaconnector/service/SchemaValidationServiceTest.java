package com.company.kafkaconnector.service;

import com.company.kafkaconnector.exception.SchemaValidationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for SchemaValidationService
 */
class SchemaValidationServiceTest {

    private SchemaValidationService schemaValidationService;

    @BeforeEach
    void setUp() {
        schemaValidationService = new SchemaValidationService();
    }

    @Test
    void shouldValidateValidUserEventMessage() throws Exception {
        // Given
        String schemaPath = "schemas/user-events-schema.json";
        
        String validMessageJson = """
            {
                "user_id": 123,
                "event_type": "login",
                "timestamp": "2023-01-01T10:00:00Z",
                "session_id": "session_123",
                "properties": {
                    "source": "web",
                    "platform": "desktop"
                }
            }
            """;

        // When & Then - should return true for valid message
        boolean result = schemaValidationService.validateMessage(schemaPath, validMessageJson);
        assertThat(result).isTrue();
    }

    @Test
    void shouldValidateValidOrderEventMessage() throws Exception {
        // Given
        String schemaPath = "schemas/order-events-schema.json";
        
        String validMessageJson = """
            {
                "order_id": "order_123",
                "user_id": 456,
                "amount": 99.99,
                "currency": "USD",
                "status": "completed",
                "timestamp": "2023-01-01T10:00:00Z",
                "items": [{
                    "product_id": "prod_123",
                    "quantity": 2,
                    "price": 49.99
                }]
            }
            """;

        // When & Then - should return true for valid message
        boolean result = schemaValidationService.validateMessage(schemaPath, validMessageJson);
        assertThat(result).isTrue();
    }

    @Test
    void shouldRejectMessageWithMissingRequiredField() {
        // Given
        String schemaPath = "schemas/user-events-schema.json";
        
        String invalidMessageJson = """
            {
                "event_type": "login",
                "timestamp": "2023-01-01T10:00:00Z"
            }
            """;

        // When & Then - should return false for invalid message
        boolean result = schemaValidationService.validateMessage(schemaPath, invalidMessageJson);
        assertThat(result).isFalse();
    }

    @Test
    void shouldRejectMessageWithWrongDataType() {
        // Given
        String schemaPath = "schemas/user-events-schema.json";
        
        String invalidMessageJson = """
            {
                "user_id": "not_a_number",
                "event_type": "login",
                "timestamp": "2023-01-01T10:00:00Z",
                "session_id": "session_123"
            }
            """;

        // When & Then - should return false for invalid data type
        boolean result = schemaValidationService.validateMessage(schemaPath, invalidMessageJson);
        assertThat(result).isFalse();
    }

    @Test
    void shouldHandleNonExistentSchemaFile() {
        // Given
        String schemaPath = "schemas/non-existent-schema.json";
        String messageJson = "{\"key\": \"value\"}";

        // When & Then - should use default permissive schema
        boolean result = schemaValidationService.validateMessage(schemaPath, messageJson);
        assertThat(result).isTrue(); // Default schema accepts any valid JSON
    }

    @Test
    void shouldCacheSchemaAfterFirstLoad() throws Exception {
        // Given
        String schemaPath = "schemas/user-events-schema.json";
        
        String validMessageJson = """
            {
                "user_id": 123,
                "event_type": "login",
                "timestamp": "2023-01-01T10:00:00Z",
                "session_id": "session_123"
            }
            """;

        // When - validate same message twice
        boolean result1 = schemaValidationService.validateMessage(schemaPath, validMessageJson);
        boolean result2 = schemaValidationService.validateMessage(schemaPath, validMessageJson);

        // Then - both should succeed and schema should be cached
        assertThat(result1).isTrue();
        assertThat(result2).isTrue();
        assertThat(schemaValidationService.getCachedSchemaCount()).isEqualTo(1);
    }

    @Test
    void shouldReloadSchemaWhenRequested() throws Exception {
        // Given
        String schemaPath = "schemas/user-events-schema.json";
        String messageJson = "{\"user_id\": 123, \"event_type\": \"login\"}";

        // When - load schema, then reload
        schemaValidationService.validateMessage(schemaPath, messageJson); // This loads and caches
        assertThat(schemaValidationService.getCachedSchemaCount()).isEqualTo(1);
        
        schemaValidationService.reloadSchema(schemaPath); // This removes from cache
        
        // Then - schema should be removed from cache
        assertThat(schemaValidationService.getCachedSchemaCount()).isEqualTo(0);
    }

    @Test
    void shouldHandleInvalidJsonMessage() {
        // Given
        String schemaPath = "schemas/user-events-schema.json";
        String invalidJson = "{ invalid json }";

        // When & Then
        assertThatThrownBy(() -> 
                schemaValidationService.validateMessage(schemaPath, invalidJson))
                .isInstanceOf(SchemaValidationException.class)
                .hasMessageContaining("Invalid JSON format");
    }
    
    @Test
    void shouldClearAllCachedSchemas() {
        // Given
        String schemaPath1 = "schemas/user-events-schema.json";
        String schemaPath2 = "schemas/order-events-schema.json";
        String messageJson = "{\"test\": \"value\"}";
        
        // When - load multiple schemas
        schemaValidationService.validateMessage(schemaPath1, messageJson);
        schemaValidationService.validateMessage(schemaPath2, messageJson);
        assertThat(schemaValidationService.getCachedSchemaCount()).isEqualTo(2);
        
        schemaValidationService.clearSchemaCache();
        
        // Then - all schemas should be cleared
        assertThat(schemaValidationService.getCachedSchemaCount()).isEqualTo(0);
    }
}