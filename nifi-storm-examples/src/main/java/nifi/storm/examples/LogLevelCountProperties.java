package nifi.storm.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author bbende.
 */
public class LogLevelCountProperties {

    static final String NIFI_URL = "nifi.url";
    static final String NIFI_INPUT_PORT = "nifi.input.port";
    static final String NIFI_REQUEST_BATCH = "nifi.input.request.batch";
    static final String NIFI_OUTPUT_PORT = "nifi.output.port";

    static final String STORM_WINDOW_MILLIS = "storm.window.milliseconds";
    static final String STORM_RATE_THRESHOLD = "storm.rate.threshold";

    static final String LOG_LEVEL_ATTRIBUTE = "log.level.attribute";

    private final String nifiUrl;
    private final String nifiInputPort;
    private final String nifiOutputPort;
    private final int nifiRequestBatch;
    private final int stormWindowMillis;
    private final double stormRateThreshold;
    private final String logLevelAttribute;

    public LogLevelCountProperties(final String propertiesFile) throws IOException {
        final InputStream in = this.getClass().getClassLoader().getResourceAsStream(propertiesFile);

        final Properties properties = new Properties();
        properties.load(in);

        nifiUrl = loadProperty(properties, NIFI_URL);
        nifiInputPort = loadProperty(properties, NIFI_INPUT_PORT);
        nifiOutputPort = loadProperty(properties, NIFI_OUTPUT_PORT);

        final String tempNiFiRequestBatch = loadProperty(properties, NIFI_REQUEST_BATCH);
        nifiRequestBatch = Integer.parseInt(tempNiFiRequestBatch);

        final String tempStormWindowSize = loadProperty(properties, STORM_WINDOW_MILLIS);
        stormWindowMillis = Integer.parseInt(tempStormWindowSize);

        final String tempStormRateThreshold = loadProperty(properties, STORM_RATE_THRESHOLD);
        stormRateThreshold = Double.parseDouble(tempStormRateThreshold);

        logLevelAttribute = loadProperty(properties, LOG_LEVEL_ATTRIBUTE);
    }

    private String loadProperty(final Properties properties, final String name) {
        final String value = properties.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(name + " is a required property");
        }
        return value.trim();
    }

    public String getNifiUrl() {
        return nifiUrl;
    }

    public String getNifiInputPort() {
        return nifiInputPort;
    }

    public String getNifiOutputPort() {
        return nifiOutputPort;
    }

    public int getNifiRequestBatch() {
        return nifiRequestBatch;
    }

    public int getStormWindowMillis() {
        return stormWindowMillis;
    }

    public double getStormRateThreshold() {
        return stormRateThreshold;
    }

    public String getLogLevelAttribute() {
        return logLevelAttribute;
    }

}
