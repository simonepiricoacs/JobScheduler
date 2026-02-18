package it.water.connectors.jobscheduler.service;

import it.water.connectors.jobscheduler.api.JobSchedulerRepository;
import it.water.connectors.jobscheduler.api.WaterJob;
import it.water.connectors.zookeeper.api.ZookeeperConnectorSystemApi;
import it.water.core.api.bundle.ApplicationProperties;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobSchedulerSystemServiceImplTest {

    @Mock
    private ZookeeperConnectorSystemApi zookeeperConnectorSystemApi;
    @Mock
    private JobSchedulerRepository repository;
    @Mock
    private JobSchedulerLeadershipRegistrar leadershipRegistrar;
    @Mock
    private Scheduler scheduler;
    @Mock
    private StdSchedulerFactory schedulerFactory;

    private JobSchedulerSystemServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        service = new JobSchedulerSystemServiceImpl() {
            @Override
            protected StdSchedulerFactory getSchedulerFactory() {
                return schedulerFactory;
            }
        };
        service.setZookeeperConnectorSystemApi(zookeeperConnectorSystemApi);
        service.setRepository(repository);
        service.setJobSchedulerLeadershipRegistrar(leadershipRegistrar);
        setPrivateField(service, "scheduler", scheduler);
    }

    @Test
    void addJobShouldThrowWhenDetailIsNull() {
        WaterJob job = mock(WaterJob.class);
        when(job.getJobDetail()).thenReturn(null);
        Assertions.assertThrows(RuntimeException.class, () -> service.addJob(job));
    }

    @Test
    void addJobShouldAddAndScheduleWhenActive() throws Exception {
        JobKey jobKey = new JobKey("j1", "g1");
        WaterJob job = mockJob(jobKey, true, "0 0/5 * * * ?");
        when(scheduler.checkExists(jobKey)).thenReturn(false);
        when(scheduler.checkExists(new TriggerKey("j1", "g1"))).thenReturn(false);

        service.addJob(job);

        verify(scheduler).addJob(job.getJobDetail(), false);
        verify(scheduler).scheduleJob(any(Trigger.class));
    }

    @Test
    void addJobShouldNotRescheduleWhenAlreadyExists() throws Exception {
        JobKey jobKey = new JobKey("j2", "g2");
        WaterJob job = mockJob(jobKey, true, "0 0/5 * * * ?");
        when(scheduler.checkExists(jobKey)).thenReturn(true);

        service.addJob(job);

        verify(scheduler, never()).addJob(any(JobDetail.class), anyBoolean());
        verify(scheduler, never()).scheduleJob(any(Trigger.class));
        verify(scheduler, never()).rescheduleJob(any(TriggerKey.class), any(Trigger.class));
    }

    @Test
    void addJobShouldThrowWhenCronIsInvalid() throws Exception {
        JobKey jobKey = new JobKey("j3", "g3");
        WaterJob job = mockJob(jobKey, true, "INVALID_CRON");
        when(scheduler.checkExists(jobKey)).thenReturn(false);
        Assertions.assertThrows(RuntimeException.class, () -> service.addJob(job));
    }

    @Test
    void addJobShouldThrowWhenSchedulerFails() throws Exception {
        JobKey jobKey = new JobKey("j4", "g4");
        WaterJob job = mockJob(jobKey, true, "0 0/5 * * * ?");
        when(scheduler.checkExists(jobKey)).thenThrow(new SchedulerException("boom"));
        Assertions.assertThrows(RuntimeException.class, () -> service.addJob(job));
    }

    @Test
    void updateJobShouldThrowWhenDetailIsNull() {
        WaterJob job = mock(WaterJob.class);
        when(job.getJobDetail()).thenReturn(null);
        Assertions.assertThrows(RuntimeException.class, () -> service.updateJob(job));
    }

    @Test
    void updateJobShouldAddAndScheduleWhenExistsAndActive() throws Exception {
        JobKey jobKey = new JobKey("j5", "g5");
        WaterJob job = mockJob(jobKey, true, "0 0/5 * * * ?");
        TriggerKey triggerKey = new TriggerKey("j5", "g5");
        when(scheduler.checkExists(jobKey)).thenReturn(true);
        when(scheduler.checkExists(triggerKey)).thenReturn(false);

        service.updateJob(job);

        verify(scheduler).addJob(job.getJobDetail(), true);
        verify(scheduler).scheduleJob(any(Trigger.class));
    }

    @Test
    void updateJobShouldRescheduleWhenTriggerExists() throws Exception {
        JobKey jobKey = new JobKey("j6", "g6");
        WaterJob job = mockJob(jobKey, true, "0 0/5 * * * ?");
        TriggerKey triggerKey = new TriggerKey("j6", "g6");
        Trigger oldTrigger = mock(Trigger.class);
        TriggerBuilder<?> builder = TriggerBuilder.newTrigger().withIdentity(triggerKey);
        when(oldTrigger.getTriggerBuilder()).thenReturn((TriggerBuilder) builder);
        when(scheduler.checkExists(jobKey)).thenReturn(true);
        when(scheduler.checkExists(triggerKey)).thenReturn(true);
        when(scheduler.getTrigger(triggerKey)).thenReturn(oldTrigger);

        service.updateJob(job);

        verify(scheduler).rescheduleJob(eq(triggerKey), any(Trigger.class));
    }

    @Test
    void updateJobShouldUnscheduleWhenExistsAndInactive() throws Exception {
        JobKey jobKey = new JobKey("j7", "g7");
        WaterJob job = mockJob(jobKey, false, "0 0/5 * * * ?");
        TriggerKey triggerKey = new TriggerKey("j7", "g7");
        when(scheduler.checkExists(jobKey)).thenReturn(true);
        when(scheduler.checkExists(triggerKey)).thenReturn(true);

        service.updateJob(job);

        verify(scheduler).addJob(job.getJobDetail(), true);
        verify(scheduler).unscheduleJob(triggerKey);
    }

    @Test
    void updateJobShouldNotUnscheduleWhenTriggerDoesNotExist() throws Exception {
        JobKey jobKey = new JobKey("j8", "g8");
        WaterJob job = mockJob(jobKey, false, "0 0/5 * * * ?");
        TriggerKey triggerKey = new TriggerKey("j8", "g8");
        when(scheduler.checkExists(jobKey)).thenReturn(true);
        when(scheduler.checkExists(triggerKey)).thenReturn(false);

        service.updateJob(job);

        verify(scheduler, never()).unscheduleJob(triggerKey);
    }

    @Test
    void updateJobShouldHandleMissingJob() throws Exception {
        JobKey jobKey = new JobKey("j9", "g9");
        WaterJob job = mockJob(jobKey, true, "0 0/5 * * * ?");
        when(scheduler.checkExists(jobKey)).thenReturn(false);

        service.updateJob(job);

        verify(scheduler, never()).addJob(any(JobDetail.class), anyBoolean());
    }

    @Test
    void updateJobShouldThrowWhenSchedulerFails() throws Exception {
        JobKey jobKey = new JobKey("j10", "g10");
        WaterJob job = mockJob(jobKey, true, "0 0/5 * * * ?");
        when(scheduler.checkExists(jobKey)).thenThrow(new SchedulerException("boom"));
        Assertions.assertThrows(RuntimeException.class, () -> service.updateJob(job));
    }

    @Test
    void deleteJobShouldThrowWhenKeyIsNull() {
        WaterJob job = mock(WaterJob.class);
        when(job.getJobKey()).thenReturn(null);
        Assertions.assertThrows(RuntimeException.class, () -> service.deleteJob(job));
    }

    @Test
    void deleteJobShouldDeleteWhenExists() throws Exception {
        JobKey jobKey = new JobKey("j11", "g11");
        WaterJob job = mock(WaterJob.class);
        lenient().when(job.getJobKey()).thenReturn(jobKey);
        when(scheduler.checkExists(jobKey)).thenReturn(true);

        service.deleteJob(job);

        verify(scheduler).deleteJob(jobKey);
    }

    @Test
    void deleteJobShouldDoNothingWhenMissing() throws Exception {
        JobKey jobKey = new JobKey("j12", "g12");
        WaterJob job = mock(WaterJob.class);
        lenient().when(job.getJobKey()).thenReturn(jobKey);
        when(scheduler.checkExists(jobKey)).thenReturn(false);

        service.deleteJob(job);

        verify(scheduler, never()).deleteJob(jobKey);
    }

    @Test
    void deleteJobShouldThrowWhenSchedulerFails() throws Exception {
        JobKey jobKey = new JobKey("j13", "g13");
        WaterJob job = mock(WaterJob.class);
        lenient().when(job.getJobKey()).thenReturn(jobKey);
        when(scheduler.checkExists(jobKey)).thenThrow(new SchedulerException("boom"));
        Assertions.assertThrows(RuntimeException.class, () -> service.deleteJob(job));
    }

    @Test
    void onDeactivateShouldShutdownSchedulerWhenPresent() throws Exception {
        service.onDeactivate();
        verify(scheduler).shutdown();
    }

    @Test
    void onDeactivateShouldHandleShutdownException() throws Exception {
        doThrow(new SchedulerException("boom")).when(scheduler).shutdown();
        service.onDeactivate();
        verify(scheduler).shutdown();
    }

    @Test
    void onActivateShouldInitializeScheduler() throws Exception {
        ApplicationProperties props = mock(ApplicationProperties.class);
        when(schedulerFactory.getScheduler()).thenReturn(scheduler);
        when(leadershipRegistrar.getLeadershipPath()).thenReturn("/test/path");
        when(zookeeperConnectorSystemApi.isLeader("/test/path")).thenReturn(true);

        service.onActivate(props);

        verify(repository).createQuartzTableIfNotExists(any());
        verify(scheduler).start();
    }

    @Test
    void leaderListenerShouldStartAndStandbyScheduler() throws Exception {
        ApplicationProperties props = mock(ApplicationProperties.class);
        when(schedulerFactory.getScheduler()).thenReturn(scheduler);
        when(leadershipRegistrar.getLeadershipPath()).thenReturn("/layer/jobs/quartz/executor");

        service.onActivate(props);

        ArgumentCaptor<LeaderLatchListener> listenerCaptor = ArgumentCaptor.forClass(LeaderLatchListener.class);
        verify(zookeeperConnectorSystemApi).addListener(listenerCaptor.capture(), eq("/layer/jobs/quartz/executor"));

        LeaderLatchListener listener = listenerCaptor.getValue();
        listener.isLeader();
        listener.notLeader();

        verify(scheduler, atLeastOnce()).start();
        verify(scheduler).standby();
    }

    @Test
    void leaderListenerShouldHandleSchedulerExceptions() throws Exception {
        ApplicationProperties props = mock(ApplicationProperties.class);
        when(schedulerFactory.getScheduler()).thenReturn(scheduler);
        when(leadershipRegistrar.getLeadershipPath()).thenReturn("/layer/jobs/quartz/executor");
        doThrow(new SchedulerException("boom")).when(scheduler).start();
        doThrow(new SchedulerException("boom")).when(scheduler).standby();

        service.onActivate(props);

        ArgumentCaptor<LeaderLatchListener> listenerCaptor = ArgumentCaptor.forClass(LeaderLatchListener.class);
        verify(zookeeperConnectorSystemApi).addListener(listenerCaptor.capture(), eq("/layer/jobs/quartz/executor"));

        LeaderLatchListener listener = listenerCaptor.getValue();
        listener.isLeader();
        listener.notLeader();

        verify(scheduler, atLeastOnce()).start();
        verify(scheduler).standby();
    }

    private WaterJob mockJob(JobKey jobKey, boolean active, String cron) {
        WaterJob job = mock(WaterJob.class);
        JobDetail detail = JobBuilder.newJob(NoOpQuartzJob.class).withIdentity(jobKey).build();
        when(job.getJobDetail()).thenReturn(detail);
        lenient().when(job.getJobKey()).thenReturn(jobKey);
        lenient().when(job.getCronExpression()).thenReturn(cron);
        lenient().when(job.isActive()).thenReturn(active);
        return job;
    }

    private static void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Class<?> clazz = target.getClass();
        Field field = null;
        while (clazz != null && clazz != Object.class) {
            try {
                field = clazz.getDeclaredField(fieldName);
                break;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        if (field == null) {
            throw new NoSuchFieldException("Field " + fieldName + " not found in hierarchy of " + target.getClass().getName());
        }
        field.setAccessible(true);
        field.set(target, value);
    }

    public static class NoOpQuartzJob implements Job {
        @Override
        public void execute(JobExecutionContext context) {
            // no-op
        }
    }
}
