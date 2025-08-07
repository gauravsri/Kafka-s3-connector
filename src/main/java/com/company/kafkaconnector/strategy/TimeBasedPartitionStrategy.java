package com.company.kafkaconnector.strategy;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Time-based partitioning strategy
 * Creates partitions based on record timestamp: topic/year=YYYY/month=MM/day=DD/hour=HH
 */
@Slf4j
public class TimeBasedPartitionStrategy implements PartitionStrategy {
    
    private final Map<String, String> config;
    private final String timeZone;
    private final DateTimeFormatter formatter;
    
    public TimeBasedPartitionStrategy(Map<String, String> config) {
        this.config = config;
        this.timeZone = config.getOrDefault("partition.timezone", "UTC");
        this.formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH").withZone(ZoneOffset.UTC);
    }
    
    @Override
    public String getPartitionKey(SinkRecord record) {
        long timestamp = record.timestamp() != null ? record.timestamp() : System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(timestamp);
        String timePartition = formatter.format(instant);
        
        return String.format("%s/year=%s/month=%s/day=%s/hour=%s", 
            record.topic(),
            timePartition.substring(0, 4),    // year
            timePartition.substring(5, 7),    // month
            timePartition.substring(8, 10),   // day
            timePartition.substring(11, 13)   // hour
        );
    }
    
    @Override
    public String getStrategyName() {
        return "TIME_BASED";
    }
}