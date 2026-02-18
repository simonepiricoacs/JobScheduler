package it.water.connectors.jobscheduler.service;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

class JobSchedulerRepositoryImplTest {

    @Test
    void createQuartzTableShouldHandleMissingClasspathResource() {
        new JobSchedulerRepositoryImpl().createQuartzTableIfNotExists(null);
    }

    @Test
    void createQuartzTableShouldReadSqlFromCustomFile() throws Exception {
        Path tmp = Files.createTempFile("quartz-test", ".sql");
        Files.writeString(tmp, "CREATE TABLE IF NOT EXISTS QRTZ_TEST(ID INT);");

        new JobSchedulerRepositoryImpl().createQuartzTableIfNotExists(tmp.toString());

        Files.deleteIfExists(tmp);
    }

    @Test
    void createQuartzTableShouldHandleInvalidCustomFilePath() {
        new JobSchedulerRepositoryImpl().createQuartzTableIfNotExists("/tmp/not-existing-quartz-file.sql");
    }
}
