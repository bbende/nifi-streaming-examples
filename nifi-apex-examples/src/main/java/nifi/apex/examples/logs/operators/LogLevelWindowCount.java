package nifi.apex.examples.logs.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.nifi.NiFiDataPacket;
import nifi.apex.examples.logs.data.LogLevels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Operator that counts the log levels over a given window. The log level is provided through
 * an attribute on each NiFiDataPacket, the name of the attribute is provided through the constructor
 * of this operator.
 */
public class LogLevelWindowCount extends BaseOperator {

    private static final Logger LOG = LoggerFactory.getLogger(LogLevelWindowCount.class);

    private final String logLevelAttribute;
    private final LogLevels logLevels = new LogLevels();

    private LogLevelWindowCount() {
        logLevelAttribute = null;
    }

    public LogLevelWindowCount(String logLevelAttribute) {
        this.logLevelAttribute = logLevelAttribute;
    }

    /**
     * Input port on which NiFiDataPackets are received.
     */
    public final transient DefaultInputPort<NiFiDataPacket> input = new DefaultInputPort<NiFiDataPacket>() {

        @Override
        public void process(NiFiDataPacket niFiDataPacket) {
            final Map<String,String> attrs = niFiDataPacket.getAttributes();

            // extract the log level and add it to the level counts for the current window
            if (attrs != null && attrs.containsKey(logLevelAttribute)) {
                logLevels.add(attrs.get(logLevelAttribute), 1);
            }
        }
    };

    /**
     * Output port which emits the map of log levels and their counts for current window
     */
    public final transient DefaultOutputPort<LogLevels> output = new DefaultOutputPort<>();

    @Override
    public void endWindow() {
        LOG.info("LogLevelWindowCount: endWindow");

        // if no counts for this window then do nothing
        if (logLevels.isEmpty()) {
            LOG.debug("Levels was empty, returning...");
            return;
        }

        // otherwise emit a copy of the counts and clear the counts for next window
        output.emit(logLevels.deepCopy());
        logLevels.clear();
    }
}
