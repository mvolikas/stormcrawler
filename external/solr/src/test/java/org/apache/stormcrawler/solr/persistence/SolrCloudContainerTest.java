package org.apache.stormcrawler.solr.persistence;

import java.io.File;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.MountableFile;

public abstract class SolrCloudContainerTest {
    private static final Logger LOG = LoggerFactory.getLogger(SolrCloudContainerTest.class);

    private static final String configsetsPath = new File("configsets").getAbsolutePath();
    private static final WaitStrategy waitStrategy =
            Wait.forHttp("/solr/admin/cores?action=STATUS").forStatusCode(200);

    @Rule public Timeout globalTimeout = Timeout.seconds(120);

    @Rule
    public ComposeContainer environment =
            new ComposeContainer(new File("src/test/resources/docker-compose.yml"))
                    .withExposedService("solr", 8983, waitStrategy)
                    .withExposedService("zookeeper", 2181);

    @Before
    public void before() {
        environment
                .getContainerByServiceName("solr")
                .ifPresent(
                        container -> {
                            container.copyFileToContainer(
                                    MountableFile.forHostPath(configsetsPath),
                                    "/opt/solr/server/solr/configsets");
                        });
    }

    protected void createCollection(String collectionName, int shards, int replicas) {
        environment
                .getContainerByServiceName("solr")
                .ifPresent(
                        container -> {
                            try {
                                // Upload configuration to Zookeeper
                                container.execInContainer(
                                        "/opt/solr/bin/solr",
                                        "zk",
                                        "upconfig",
                                        "-n",
                                        collectionName,
                                        "-d",
                                        "/opt/solr/server/solr/configsets/" + collectionName,
                                        "-z",
                                        "zookeeper:2181");

                                // Create the collection
                                container.execInContainer(
                                        "/opt/solr/bin/solr",
                                        "create",
                                        "-c",
                                        collectionName,
                                        "-n",
                                        collectionName,
                                        "-sh",
                                        String.valueOf(shards),
                                        "-rf",
                                        String.valueOf(replicas));
                            } catch (Exception e) {
                                LOG.error("Error while creating collection {}", collectionName, e);
                            }
                        });
    }
}
