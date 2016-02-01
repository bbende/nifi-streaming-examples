package nifi.flink.examples.logs;

import nifi.flink.examples.logs.data.DictionaryBuilder;
import nifi.flink.examples.logs.data.LogLevels;
import nifi.flink.examples.logs.functions.LogLevelFlatMap;
import nifi.flink.examples.logs.functions.LogLevelWindowCounter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacket;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacketBuilder;
import org.apache.flink.streaming.connectors.nifi.NiFiSink;
import org.apache.flink.streaming.connectors.nifi.NiFiSource;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

import java.util.concurrent.TimeUnit;

/**
 * Flink Streaming application that receives log data from NiFi and groups the logs by their
 * level, counting the number of logs per level over window.
 *
 * @author bbende
 */
public class WindowLogLevelCount {

    public static final String DEFAULT_PROPERTIES_FILE = "window-log-level.properties";

    public static void main(String[] args) throws Exception {
        String propertiesFile = DEFAULT_PROPERTIES_FILE;
        if (args != null && args.length == 1 && args[0] != null) {
            propertiesFile = args[0];
        }

        WindowLogLevelCountProps props = new WindowLogLevelCountProps(propertiesFile);

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure the SiteToSiteClient
        SiteToSiteClientConfig clientConfig = getSourceConfig(props);

        // Create our data stream with a NiFiSource
        SourceFunction<NiFiDataPacket> nifiSource = new NiFiSource(clientConfig);
        DataStream<NiFiDataPacket> streamSource = env.addSource(nifiSource);

        int windowSize = props.getFlinkWindowMillis();
        LogLevelFlatMap logLevelFlatMap = new LogLevelFlatMap(props.getLogLevelAttribute());

        // Count the occurrences of each log level over a window
        DataStream<LogLevels> counts =
                streamSource.flatMap(logLevelFlatMap)
                        .timeWindowAll(Time.of(windowSize, TimeUnit.MILLISECONDS))
                        .apply(new LogLevelWindowCounter());

        // Add the sink to send the dictionary back to NiFi
        double rateThreshold = props.getFlinkRateThreshold();
        SiteToSiteClientConfig sinkConfig = getSinkConfig(props);
        NiFiDataPacketBuilder<LogLevels> builder = new DictionaryBuilder(windowSize, rateThreshold);
        counts.addSink(new NiFiSink<>(sinkConfig, builder));

        // execute program
        env.execute("WindowLogLevelCount");
    }

    private static SiteToSiteClientConfig getSourceConfig(WindowLogLevelCountProps props) {
        return new SiteToSiteClient.Builder()
                .url(props.getNifiUrl())
                .portName(props.getNifiInputPort())
                .requestBatchCount(props.getNifiRequestBatch())
                .buildConfig();
    }

    private static SiteToSiteClientConfig getSinkConfig(WindowLogLevelCountProps props) {
        return new SiteToSiteClient.Builder()
                .url(props.getNifiUrl())
                .portName(props.getNifiOutputPort())
                .buildConfig();
    }

}
