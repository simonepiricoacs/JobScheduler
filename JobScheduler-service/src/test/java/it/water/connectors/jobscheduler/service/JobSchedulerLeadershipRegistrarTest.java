package it.water.connectors.jobscheduler.service;

import it.water.connectors.zookeeper.api.ZookeeperConnectorSystemApi;
import it.water.core.api.bundle.ApplicationProperties;
import it.water.core.api.service.cluster.ClusterNodeOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobSchedulerLeadershipRegistrarTest {

    @Mock
    private ClusterNodeOptions clusterNodeOptions;
    @Mock
    private ZookeeperConnectorSystemApi zookeeperConnectorSystemApi;
    @Mock
    private ApplicationProperties applicationProperties;

    @Test
    void activateShouldBuildLeadershipPathAndRegisterIt() {
        JobSchedulerLeadershipRegistrar registrar = new JobSchedulerLeadershipRegistrar();
        registrar.setClusterNodeOptions(clusterNodeOptions);
        registrar.setZookeeperConnectorSystemApi(zookeeperConnectorSystemApi);

        when(clusterNodeOptions.getLayer()).thenReturn("test-layer");

        registrar.activate(applicationProperties);

        Assertions.assertEquals("/test-layer/jobs/quartz/executor", registrar.getLeadershipPath());
        verify(zookeeperConnectorSystemApi).registerLeadershipComponent("/test-layer/jobs/quartz/executor");
    }
}
