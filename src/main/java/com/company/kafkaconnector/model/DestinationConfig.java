package com.company.kafkaconnector.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public class DestinationConfig {
    @NotBlank
    private String bucket;
    
    @NotBlank
    private String path;
    
    @NotBlank
    private String tableName;
    
    @NotEmpty
    private List<String> partitionColumns;
    
    @Valid
    private DeltaConfig deltaConfig = new DeltaConfig();

    // Constructors
    public DestinationConfig() {}

    public DestinationConfig(String bucket, String path, String tableName, List<String> partitionColumns, DeltaConfig deltaConfig) {
        this.bucket = bucket;
        this.path = path;
        this.tableName = tableName;
        this.partitionColumns = partitionColumns;
        this.deltaConfig = deltaConfig;
    }

    // Getters and Setters
    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(List<String> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    public DeltaConfig getDeltaConfig() {
        return deltaConfig;
    }

    public void setDeltaConfig(DeltaConfig deltaConfig) {
        this.deltaConfig = deltaConfig;
    }

    public String getFullPath() {
        return "s3a://" + bucket + "/" + path;
    }
}