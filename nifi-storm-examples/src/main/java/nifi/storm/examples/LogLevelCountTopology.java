package nifi.storm.examples;

import nifi.storm.examples.bolt.LogLevelWindowBolt;
import nifi.storm.examples.data.DictionaryBuilder;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.storm.NiFiBolt;
import org.apache.nifi.storm.NiFiDataPacketBuilder;
import org.apache.nifi.storm.NiFiSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Storm topology that receives log data from NiFi and counts the number of logs per level over a window,
 * sending a new dictionary file back to NiFi based on a rate threshold.
 *
 * @author bbende.
 */
public class LogLevelCountTopology {

    public static final String DEFAULT_PROPERTIES_FILE = "log-level-count.properties";

    public static void main( String[] args ) throws Exception {
        String propertiesFile = DEFAULT_PROPERTIES_FILE;
        if (args != null && args.length == 1 && args[0] != null) {
            propertiesFile = args[0];
        }

        LogLevelCountProperties props = new LogLevelCountProperties(propertiesFile);

        int windowMillis = props.getStormWindowMillis();
        double rateThreshold = props.getStormRateThreshold();

        // Build the spout for pulling data from NiFi and pull out the log level into a tuple field
        NiFiSpout niFiSpout = new NiFiSpout(getSourceConfig(props), Collections.singletonList(props.getLogLevelAttribute()));

        // Build the bolt for counting log levels over a tumbling window
        BaseWindowedBolt logLevelWindowBolt = new LogLevelWindowBolt(props.getLogLevelAttribute())
                .withTumblingWindow(new BaseWindowedBolt.Duration(windowMillis, TimeUnit.MILLISECONDS));

        // Build the bolt for pushing results back to NiFi
        NiFiDataPacketBuilder dictionaryBuilder = new DictionaryBuilder(windowMillis, rateThreshold);
        NiFiBolt niFiBolt = new NiFiBolt(getSinkConfig(props), dictionaryBuilder, 10).withBatchSize(1);

        // Build the topology of NiFiSpout -> LogLevelWindowBolt -> NiFiBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("nifiInput", niFiSpout);
        builder.setBolt("logLevels", logLevelWindowBolt).shuffleGrouping("nifiInput");
        builder.setBolt("nifiOutput", niFiBolt).shuffleGrouping("logLevels");

        // Submit the topology
        Config conf = new Config();
        conf.setDebug(true);

        // Need to set the message timeout to twice the window size in seconds
        conf.setMessageTimeoutSecs((props.getStormWindowMillis()/1000) * 2);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("log-levels", conf, builder.createTopology());
            Utils.sleep(130000);
            cluster.killTopology("log-levels");
            cluster.shutdown();
        }
    }

    private static SiteToSiteClientConfig getSourceConfig(LogLevelCountProperties props) {
        return new SiteToSiteClient.Builder()
                .url(props.getNifiUrl())
                .portName(props.getNifiInputPort())
                .requestBatchCount(props.getNifiRequestBatch())
                .buildConfig();
    }

    private static SiteToSiteClientConfig getSinkConfig(LogLevelCountProperties props) {
        return new SiteToSiteClient.Builder()
                .url(props.getNifiUrl())
                .portName(props.getNifiOutputPort())
                .buildConfig();
    }

}
