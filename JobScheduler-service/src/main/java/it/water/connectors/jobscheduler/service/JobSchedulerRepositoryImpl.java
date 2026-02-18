package it.water.connectors.jobscheduler.service;

import it.water.connectors.jobscheduler.api.JobSchedulerRepository;
import it.water.core.interceptors.annotations.FrameworkComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

/**
 * Repository implementation for JobScheduler.
 * This is not related to a JPA Entity; it is used just for interacting
 * with the Quartz database to create scheduler tables.
 */
@FrameworkComponent
public class JobSchedulerRepositoryImpl implements JobSchedulerRepository {
    public static final String QUARTZ_CREATION_SQL_POSTGRES_FILE = "quartz_creation_postgres.sql";
    private static final Logger logger = LoggerFactory.getLogger(JobSchedulerRepositoryImpl.class);

    @Override
    public void createQuartzTableIfNotExists(String filePath) {
        try {
            InputStream is;
            if (filePath == null)
                is = this.getClass().getClassLoader().getResourceAsStream(QUARTZ_CREATION_SQL_POSTGRES_FILE);
            else
                is = new FileInputStream(filePath);
            if (is != null) {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String sql = br.lines().collect(Collectors.joining("\n"));
                logger.info("Quartz SQL script loaded ({} chars). Execution depends on DataSource availability.", sql.length());
                br.close();
                is.close();
            } else {
                logger.warn("Quartz SQL script not found, skipping table creation");
            }
        } catch (Exception t) {
            logger.error(t.getMessage(), t);
        }
    }
}
