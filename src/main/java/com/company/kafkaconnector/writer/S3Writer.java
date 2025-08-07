package com.company.kafkaconnector.writer;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

/**
 * Interface for S3 writers
 * Following Interface Segregation Principle
 */
public interface S3Writer extends AutoCloseable {
    
    /**
     * Write a batch of records to S3
     * @param records List of records to write
     * @throws Exception if write operation fails
     */
    void write(List<SinkRecord> records) throws Exception;
    
    /**
     * Flush any pending data to S3
     * @throws Exception if flush operation fails
     */
    void flush() throws Exception;
    
    /**
     * Get the current file path being written to
     * @return Current file path in S3
     */
    String getCurrentFilePath();
    
    /**
     * Get statistics about the writer
     * @return Writer statistics
     */
    WriterStats getStats();
}