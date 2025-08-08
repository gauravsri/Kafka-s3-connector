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
                "user_id": "user_123",
                "event_type": "login",
                "timestamp": "2023-01-01T10:00:00Z",
                "session_id": "session_abc"
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
                "order_id": "order_xyz",
                "customer_id": "cust_456",
                "order_status": "created",
                "timestamp": "2023-01-01T11:00:00Z",
                "total_amount": 150.75,
                "currency": "EUR",
                "items": [
                    {
                        "product_id": "prod_abc",
                        "quantity": 3,
                        "unit_price": 50.25
                    }
                ]
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
                "user_id": 12345,
                "event_type": "click",
                "timestamp": "2023-01-01T12:00:00Z"
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
                "user_id": "user_123",
                "event_type": "login",
                "timestamp": "2023-01-01T10:00:00Z",
                "session_id": "session_abc"
            }
            """;

        // When - validate same message twice
        schemaValidationService.validateMessage(schemaPath, validMessageJson);
        schemaValidationService.validateMessage(schemaPath, validMessageJson);

        // Then - schema should be cached
        assertThat(schemaValidationService.isSchemaCached(schemaPath)).isTrue();
        assertThat(schemaValidationService.getCachedSchemaCount()).isEqualTo(1);
    }

    @Test
    void shouldReloadSchemaWhenRequested() throws Exception {
        // Given
        String schemaPath = "schemas/user-events-schema.json";
        String messageJson = "{\"user_id\": \"user_123\", \"event_type\": \"login\", \"timestamp\": \"2023-01-01T10:00:00Z\"}";

        // When - load schema, then reload
        schemaValidationService.validateMessage(schemaPath, messageJson); // This loads and caches
        assertThat(schemaValidationService.isSchemaCached(schemaPath)).isTrue();

        schemaValidationService.reloadSchema(schemaPath); // This removes from cache

        // Then - schema should be removed from cache
        assertThat(schemaValidationService.isSchemaCached(schemaPath)).isFalse();
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