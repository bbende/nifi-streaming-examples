package nifi.flink.examples.logs;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Properties for WindowLogLevelAnalytic.
 */
public class WindowLogLevelCountProps {

    static final String NIFI_URL = "nifi.url";
    static final String NIFI_INPUT_PORT = "nifi.input.port";
    static final String NIFI_REQUEST_BATCH = "nifi.input.request.batch";
    static final String NIFI_OUTPUT_PORT = "nifi.output.port";

    static final String FLINK_WINDOW_MILLIS = "flink.window.milliseconds";
    static final String FLINK_SLIDE_MILLIS = "flink.slide.milliseconds";
    static final String FLINK_RATE_THRESHOLD = "flink.rate.threshold";

    static final String FLINK_OUTPUT_PATH = "flink.output.path";
    static final String FLINK_OUTPUT_FILENAME = "flink.output.filename";
    static final String LOG_LEVEL_ATTRIBUTE = "log.level.attribute";

    private final String nifiUrl;
    private final String nifiInputPort;
    private final String nifiOutputPort;
    private final int nifiRequestBatch;
    private final int flinkWindowMillis;
    private final int flinkSlideMillis;
    private final double flinkRateThreshold;
    private final String flinkOutputPath;
    private final String flinkOutputFileName;
    private final String logLevelAttribute;

    public WindowLogLevelCountProps(final String propertiesFile) throws IOException {
        final InputStream in = this.getClass().getClassLoader().getResourceAsStream(propertiesFile);

        final Properties properties = new Properties();
        properties.load(in);

        nifiUrl = loadProperty(properties, NIFI_URL);
        nifiInputPort = loadProperty(properties, NIFI_INPUT_PORT);
        nifiOutputPort = loadProperty(properties, NIFI_OUTPUT_PORT);

        final String tempNiFiRequestBatch = loadProperty(properties, NIFI_REQUEST_BATCH);
        nifiRequestBatch = Integer.parseInt(tempNiFiRequestBatch);

        final String tempflinkWindowSize = loadProperty(properties, FLINK_WINDOW_MILLIS);
        flinkWindowMillis = Integer.parseInt(tempflinkWindowSize);

        final String tempFlinkSlideSize = loadProperty(properties, FLINK_SLIDE_MILLIS);
        flinkSlideMillis = Integer.parseInt(tempFlinkSlideSize);

        final String tempFlinkRateThreshold = loadProperty(properties, FLINK_RATE_THRESHOLD);
        flinkRateThreshold = Double.parseDouble(tempFlinkRateThreshold);

        flinkOutputPath = loadProperty(properties, FLINK_OUTPUT_PATH);
        flinkOutputFileName = loadProperty(properties, FLINK_OUTPUT_FILENAME);

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

    public int getFlinkWindowMillis() {
        return flinkWindowMillis;
    }

    public int getFlinkSlideMillis() {
        return flinkSlideMillis;
    }

    public double getFlinkRateThreshold() {
        return flinkRateThreshold;
    }

    public String getFlinkOutputPath() {
        return flinkOutputPath;
    }

    public String getFlinkOutputFileName() {
        return flinkOutputFileName;
    }

    public String getLogLevelAttribute() {
        return logLevelAttribute;
    }
}
