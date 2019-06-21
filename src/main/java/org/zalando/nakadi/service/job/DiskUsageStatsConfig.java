package org.zalando.nakadi.service.job;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "nakadi.jobs.diskUsageStats")
public class DiskUsageStatsConfig {
    private String authDataType;
    private String authValue;
    private String owningApplication;
    private String eventTypeName;
    private long runPeriodMs;

    public String getAuthDataType() {
        return authDataType;
    }

    public void setAuthDataType(String authDataType) {
        this.authDataType = authDataType;
    }

    public String getAuthValue() {
        return authValue;
    }

    public void setAuthValue(String authValue) {
        this.authValue = authValue;
    }

    public String getOwningApplication() {
        return owningApplication;
    }

    public void setOwningApplication(String owningApplication) {
        this.owningApplication = owningApplication;
    }

    public String getEventTypeName() {
        return eventTypeName;
    }

    public void setEventTypeName(String eventTypeName) {
        this.eventTypeName = eventTypeName;
    }

    public long getRunPeriodMs() {
        return runPeriodMs;
    }

    public void setRunPeriodMs(long runPeriodMs) {
        this.runPeriodMs = runPeriodMs;
    }
}
