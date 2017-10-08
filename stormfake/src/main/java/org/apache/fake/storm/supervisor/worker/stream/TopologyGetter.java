package org.apache.fake.storm.supervisor.worker.stream;

import org.apache.fake.storm.generated.FileOperationException;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.utils.Serde;
import org.apache.storm.generated.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by yilong on 2017/10/6.
 */
public class TopologyGetter {
    public static final Logger LOG = LoggerFactory.getLogger(TopologyGetter.class);

    final private String stormId;
    final private String topoId;
    private final IResourceStore resourceStore;

    public TopologyGetter(String stormId, final IResourceStore resourceStore) {
        this(stormId, "wordcount", resourceStore);
    }

    public TopologyGetter(String stormId, String topoId, final IResourceStore resourceStore) {
        this.stormId = stormId;
        this.topoId = topoId;
        this.resourceStore = resourceStore;
    }

    public StormTopology get() throws FileOperationException {
        try {
            String topo = resourceStore.getTopoSerial(stormId, topoId);
            StormTopology topology = new TopologyBuilder().createTopology();
            topology = (StormTopology) Serde.thriftObjDeSerialize(topo, topology);

            return topology;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new FileOperationException("StormTopology_get failed : " + e.getMessage());
        }
    }
}
