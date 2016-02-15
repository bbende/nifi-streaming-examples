package nifi.apex.examples.logs;

import org.apache.hadoop.conf.Configuration;

public class LogLevelProperties {

    public static final String NIFI_URL = "nifi.url";
    public static final String NIFI_INPUT_PORT = "nifi.input.port";
    public static final String NIFI_INPUT_REQUEST_BATCH = "nifi.input.request.batch";
    public static final String NIFI_OUTPUT_PORT = "nifi.output.port";
    public static final String LOG_LEVEL_ATTRIBUTE = "log.level.attribute";
    public static final String LOG_LEVEL_THRESHOLD = "log.level.rate.threshold";
    public static final String WINDOW_SIZE_MILLIS = "window.size.millis";
    public static final String APP_WINDOW_COUNT = "application.window.count";

    private final String nifiUrl;
    private final String nifiInputPort;
    private final String nifiOutputPort;
    private final int nifiRequestBatch;
    private final double logLevelThreshold;
    private final String logLevelAttribute;
    private final int windowMillis;
    private final int appWindowCount;

    public LogLevelProperties(Configuration conf) {
        nifiUrl = loadProperty(conf, NIFI_URL);
        nifiInputPort = loadProperty(conf, NIFI_INPUT_PORT);
        nifiOutputPort = loadProperty(conf, NIFI_OUTPUT_PORT);

        final String tempNiFiRequestBatch = loadProperty(conf, NIFI_INPUT_REQUEST_BATCH);
        nifiRequestBatch = Integer.parseInt(tempNiFiRequestBatch);

        final String tempLogLevelThreshold = loadProperty(conf, LOG_LEVEL_THRESHOLD);
        logLevelThreshold = Double.parseDouble(tempLogLevelThreshold);

        logLevelAttribute = loadProperty(conf, LOG_LEVEL_ATTRIBUTE);

        final String tempWindowMillis = loadProperty(conf, WINDOW_SIZE_MILLIS);
        windowMillis = Integer.parseInt(tempWindowMillis);

        final String tempAppWindowCount = loadProperty(conf, APP_WINDOW_COUNT);
        appWindowCount = Integer.parseInt(tempAppWindowCount);
    }

    private String loadProperty(final Configuration conf, final String name) {
        final String value = conf.get(name);
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

    public double getLogLevelThreshold() {
        return logLevelThreshold;
    }

    public String getLogLevelAttribute() {
        return logLevelAttribute;
    }

    public int getWindowMillis() {
        return windowMillis;
    }

    public int getAppWindowCount() {
        return appWindowCount;
    }
}
