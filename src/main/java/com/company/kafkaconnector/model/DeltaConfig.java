package com.company.kafkaconnector.model;

public class DeltaConfig {
    private boolean enableOptimize = true;
    private int optimizeInterval = 10; // batches
    private boolean enableVacuum = false;
    private int vacuumRetentionHours = 168; // 7 days
    private boolean enableSchemaEvolution = true;
    private String checkpointInterval = "10";

    // Constructors
    public DeltaConfig() {}

    public DeltaConfig(boolean enableOptimize, int optimizeInterval, boolean enableVacuum, 
                      int vacuumRetentionHours, boolean enableSchemaEvolution, String checkpointInterval) {
        this.enableOptimize = enableOptimize;
        this.optimizeInterval = optimizeInterval;
        this.enableVacuum = enableVacuum;
        this.vacuumRetentionHours = vacuumRetentionHours;
        this.enableSchemaEvolution = enableSchemaEvolution;
        this.checkpointInterval = checkpointInterval;
    }

    // Getters and Setters
    public boolean isEnableOptimize() {
        return enableOptimize;
    }

    public void setEnableOptimize(boolean enableOptimize) {
        this.enableOptimize = enableOptimize;
    }

    public int getOptimizeInterval() {
        return optimizeInterval;
    }

    public void setOptimizeInterval(int optimizeInterval) {
        this.optimizeInterval = optimizeInterval;
    }

    public boolean isEnableVacuum() {
        return enableVacuum;
    }

    public void setEnableVacuum(boolean enableVacuum) {
        this.enableVacuum = enableVacuum;
    }

    public int getVacuumRetentionHours() {
        return vacuumRetentionHours;
    }

    public void setVacuumRetentionHours(int vacuumRetentionHours) {
        this.vacuumRetentionHours = vacuumRetentionHours;
    }

    public boolean isEnableSchemaEvolution() {
        return enableSchemaEvolution;
    }

    public void setEnableSchemaEvolution(boolean enableSchemaEvolution) {
        this.enableSchemaEvolution = enableSchemaEvolution;
    }

    public String getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(String checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }
}