package org.kin.kafka.multithread.domain;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public class HealthReport{
    private long containerId;
    private long totalMemory;
    private long freeMemory;
    private long maxMemory;
    private int availableProcessors;

    private int appNums;
    private long containerIdleTimeout;

    public HealthReport(long containerId, int appNums, long containerIdleTimeout) {
        getSystemEnv();
        this.appNums = appNums;
        this.containerId = containerId;
        this.containerIdleTimeout = containerIdleTimeout;
    }

    private void getSystemEnv(){
        Runtime runtime = Runtime.getRuntime();
        this.totalMemory = runtime.totalMemory();
        this.freeMemory = runtime.freeMemory();
        this.maxMemory = runtime.maxMemory();
        this.availableProcessors = runtime.availableProcessors();
    }

    public long getContainerId() {
        return containerId;
    }

    public void setContainerId(long containerId) {
        this.containerId = containerId;
    }

    public long getTotalMemory() {
        return totalMemory;
    }

    public void setTotalMemory(long totalMemory) {
        this.totalMemory = totalMemory;
    }

    public long getFreeMemory() {
        return freeMemory;
    }

    public void setFreeMemory(long freeMemory) {
        this.freeMemory = freeMemory;
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(int availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    public int getAppNums() {
        return appNums;
    }

    public void setAppNums(int appNums) {
        this.appNums = appNums;
    }

    public long getContainerIdleTimeout() {
        return containerIdleTimeout;
    }

    public void setContainerIdleTimeout(long containerIdleTimeout) {
        this.containerIdleTimeout = containerIdleTimeout;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        HealthReport healthReport = new HealthReport(containerId, appNums, containerIdleTimeout);
        healthReport.setAvailableProcessors(availableProcessors);
        healthReport.setTotalMemory(totalMemory);
        healthReport.setFreeMemory(freeMemory);
        healthReport.setMaxMemory(maxMemory);

        return healthReport;
    }
}
