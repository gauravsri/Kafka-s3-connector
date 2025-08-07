package com.company.kafkaconnector.writer;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * Statistics for S3 writers
 * Following Value Object pattern with Lombok
 */
@Data
@Builder
public class WriterStats {
    private final long recordsWritten;
    private final long bytesWritten;
    private final long filesCreated;
    private final Instant lastWriteTime;
    private final Instant createdTime;
    private final long writeErrors;
    private final String currentFilePath;
}