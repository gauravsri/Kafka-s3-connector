package com.company.kafkaconnector.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for ConnectorConfiguration
 */
class ConnectorConfigurationTest {

    @Test
    void shouldBindValidConfiguration() {
        // Given
        Map<String, Object> properties = createValidProperties();
        
        // When
        ConnectorConfiguration config = bindProperties(properties);
        
        // Then
        assertThat(config).isNotNull();
        assertThat(config.getTopics()).hasSize(2);
        assertThat(config.getTopics()).containsKeys("user-events", "order-events");
        
        // Verify user-events topic config
        var userEventsConfig = config.getTopics().get("user-events");
        assertThat(userEventsConfig.getKafkaTopic()).isEqualTo("user-events");
        assertThat(userEventsConfig.getSchemaFile()).isEqualTo("schemas/user-events-schema.json");
        assertThat(userEventsConfig.getDestination().getBucket()).isEqualTo("test-data-lake");
        
        // Verify order-events topic config
        var orderEventsConfig = config.getTopics().get("order-events");
        assertThat(orderEventsConfig.getKafkaTopic()).isEqualTo("order-events");
        assertThat(orderEventsConfig.getDestination().getTableName()).isEqualTo("orders");
    }

    @Test
    void shouldValidateRequiredProperties() {
        // Given - missing required topic name
        Map<String, Object> properties = new HashMap<>();
        properties.put("connector.topics.user-events.schema-file", "schema.json");
        
        // When & Then
        assertThatThrownBy(() -> bindProperties(properties))
                .isInstanceOf(BindValidationException.class);
    }

    @Test
    void shouldSetDefaultValues() {
        // Given
        Map<String, Object> properties = createMinimalValidProperties();
        
        // When
        ConnectorConfiguration config = bindProperties(properties);
        
        // Then
        var topicConfig = config.getTopics().get("test-topic");
        assertThat(topicConfig.getProcessing().getBatchSize()).isEqualTo(1000);
        assertThat(topicConfig.getProcessing().getFlushInterval()).isEqualTo(60);
        assertThat(topicConfig.getProcessing().getMaxRetries()).isEqualTo(3);
    }

    @Test
    void shouldValidatePositiveNumbers() {
        // Given
        Map<String, Object> properties = createValidProperties();
        properties.put("connector.topics.user-events.processing.batch-size", -1);
        
        // When & Then
        assertThatThrownBy(() -> bindProperties(properties))
                .isInstanceOf(BindValidationException.class);
    }

    private Map<String, Object> createValidProperties() {
        Map<String, Object> properties = new HashMap<>();
        
        // User events topic
        properties.put("connector.topics.user-events.topic-name", "user-events");
        properties.put("connector.topics.user-events.schema-file", "schemas/user-events-schema.json");
        properties.put("connector.topics.user-events.destination.bucket", "test-data-lake");
        properties.put("connector.topics.user-events.destination.prefix", "events/user");
        properties.put("connector.topics.user-events.destination.table-name", "user_events");
        properties.put("connector.topics.user-events.processing.batch-size", 500);
        
        // Order events topic
        properties.put("connector.topics.order-events.topic-name", "order-events");
        properties.put("connector.topics.order-events.schema-file", "schemas/order-events-schema.json");
        properties.put("connector.topics.order-events.destination.bucket", "test-data-lake");
        properties.put("connector.topics.order-events.destination.prefix", "events/order");
        properties.put("connector.topics.order-events.destination.table-name", "orders");
        
        return properties;
    }

    private Map<String, Object> createMinimalValidProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("connector.topics.test-topic.topic-name", "test-topic");
        properties.put("connector.topics.test-topic.schema-file", "schema.json");
        properties.put("connector.topics.test-topic.destination.bucket", "bucket");
        return properties;
    }

    private ConnectorConfiguration bindProperties(Map<String, Object> properties) {
        MutablePropertySources propertySources = new MutablePropertySources();
        propertySources.addFirst(new MapPropertySource("test", properties));
        
        Binder binder = new Binder(ConfigurationPropertySources.from(propertySources));
        return binder.bind("connector", ConnectorConfiguration.class).get();
    }
}