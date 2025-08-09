package com.company.kafkaconnector.model;

import java.time.Instant;
import java.time.LocalDate;

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
    
    public String getTopic() { 
        return topic; 
    }
    
    public LocalDate getCobDate() { 
        return cobDate; 
    }
    
    public long getMessageCount() { 
        return messageCount; 
    }
    
    public Instant getLastActivity() { 
        return lastActivity; 
    }
    
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