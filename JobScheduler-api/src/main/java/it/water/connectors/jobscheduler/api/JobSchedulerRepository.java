package it.water.connectors.jobscheduler.api;

/**
 * Repository interface for JobScheduler.
 * Used only for Quartz table initialization.
 */
public interface JobSchedulerRepository {
    void createQuartzTableIfNotExists(String initScriptFilePath);
}
