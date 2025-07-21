package org.apache.stormcrawler.solr.persistence;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.stormcrawler.solr.SolrConnection;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudConnectionTest extends SolrCloudContainerTest {
    private static final Logger LOG = LoggerFactory.getLogger(CloudConnectionTest.class);

    private SolrConnection connection;
    private CloudHttp2SolrClient client;

    @Before
    public void setup() {
        createCollection("docs", 1, 2);

        Map<String, Object> conf = new HashMap<>();
        conf.put("solr.indexer.zkhost", "localhost:2181");
        conf.put("solr.indexer.collection", "docs");
        conf.put("solr.indexer.batchUpdateSize", 1);
        conf.put("solr.indexer.flushAfterNoUpdatesMillis", 5_000);

        connection = SolrConnection.getConnection(conf, "indexer");
        client =
                new CloudHttp2SolrClient.Builder(
                                Collections.singletonList("localhost:2181"), Optional.empty())
                        .withDefaultCollection("docs")
                        .build();
    }

    @Test
    public void consistencyTest() throws InterruptedException {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("url", "test");
        doc.addField("content", "test");
        connection.addAsync(doc);

        int leader = getLiveLeader().getNodeName().contains("8983") ? 8983 : 8984;

        // Kill the leader node
        stopNode(leader);

        // Wait for recovery
        while (getLiveLeader() == null) {
            Thread.sleep(100);
        }

        assertEquals(1, getNumFound());
    }

    private Replica getLiveLeader() {
        DocCollection col = client.getClusterState().getCollection("docs");
        Replica leader = col.getActiveSlices().stream().findFirst().orElse(null).getLeader();

        if (leader == null) {
            return null;
        }

        if (!client.getClusterState().getLiveNodes().contains(leader.getNodeName())) {
            return null;
        }

        return leader;
    }

    private void startNode(int port) {
        environment
                .getContainerByServiceName("solr")
                .ifPresent(
                        container -> {
                            try {
                                container.execInContainer(
                                        "/opt/solr/bin/solr",
                                        "start",
                                        "-c",
                                        "-p",
                                        Integer.toString(port),
                                        "-s",
                                        "/var/solr/" + port,
                                        "-z",
                                        "zookeeper:2181");
                            } catch (Exception e) {
                                LOG.error("Error while starting Solr node {}", port, e);
                            }
                        });
    }

    private void stopNode(int port) {
        environment
                .getContainerByServiceName("solr")
                .ifPresent(
                        container -> {
                            try {
                                container.execInContainer(
                                        "/opt/solr/bin/solr", "stop", "-p", Integer.toString(port));
                            } catch (Exception e) {
                                LOG.error("Error while stopping Solr node {}", port, e);
                            }
                        });
    }

    private long getNumFound() {
        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        QueryRequest queryRequest = new QueryRequest(query);
        QueryResponse response = connection.requestAsync(queryRequest).join();
        return response.getResults().getNumFound();
    }
}
