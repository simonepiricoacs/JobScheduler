package it.water.connectors.jobscheduler.service;

import it.water.connectors.zookeeper.api.ZookeeperConnectorSystemApi;
import it.water.core.api.bundle.ApplicationProperties;
import it.water.core.api.interceptors.OnActivate;
import it.water.core.api.service.cluster.ClusterNodeOptions;
import it.water.core.interceptors.annotations.FrameworkComponent;
import it.water.core.interceptors.annotations.Inject;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leadership registrar for JobScheduler.
 * Registers a leadership latch on Zookeeper to ensure that only one node
 * in the cluster runs the Quartz scheduler.
 */
@FrameworkComponent
public class JobSchedulerLeadershipRegistrar {
    private static final Logger logger = LoggerFactory.getLogger(JobSchedulerLeadershipRegistrar.class);

    @Inject
    @Setter
    private ClusterNodeOptions clusterNodeOptions;

    @Inject
    @Setter
    private ZookeeperConnectorSystemApi zookeeperConnectorSystemApi;

    private String leadershipPath;

    @OnActivate
    public void activate(ApplicationProperties applicationProperties) {
        String layer = clusterNodeOptions.getLayer();
        leadershipPath = "/" + layer + "/jobs/quartz/executor";
        logger.info("*** WATER JOB SCHEDULER CLUSTER SCHEDULER LEADERSHIP PATH: {} ***", leadershipPath);
        zookeeperConnectorSystemApi.registerLeadershipComponent(leadershipPath);
    }

    public String getLeadershipPath() {
        return leadershipPath;
    }
}
