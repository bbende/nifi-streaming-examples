package nifi.flink.examples.logs.functions;

import nifi.flink.examples.logs.data.LogLevel;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacket;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * A FlatMapFunction that maps a NiFiDataPacket to a tuple of (log-level,1).
 */
public final class LogLevelFlatMap implements FlatMapFunction<NiFiDataPacket, LogLevel> {
    private static final long serialVersionUID = 1L;

    private final String attributeName;

    /**
     * @param attributeName the name of an attribute on the NiFiDataPacket containing the log level
     */
    public LogLevelFlatMap(final String attributeName) {
        this.attributeName = attributeName;
    }

    @Override
    public void flatMap(NiFiDataPacket niFiDataPacket, Collector<LogLevel> collector)
            throws Exception {

        Map<String,String> attributes = niFiDataPacket.getAttributes();

        if (attributes.containsKey(attributeName)) {
            String logLevel = niFiDataPacket.getAttributes().get(attributeName);
            collector.collect(new LogLevel(logLevel, 1));
        }
    }
}
