package it.water.connectors.jobscheduler.model;

/**
 * Constants for JobScheduler configuration properties.
 */
public final class JobSchedulerConstants {

    private JobSchedulerConstants() {
    }

    /**
     * Configuration PID for JobScheduler properties
     */
    public static final String JOB_SCHEDULER_CONFIG_PID = "it.water.connectors.jobscheduler";

    /**
     * Property key for custom SQL init script path
     */
    public static final String JOB_SCHEDULER_INIT_SCRIPT = "it.water.connectors.jobscheduler.init.script";

    /**
     * Prefix for Quartz properties that will be passed to StdSchedulerFactory
     */
    public static final String QUARTZ_PROPERTY_PREFIX = "org.quartz";

}
