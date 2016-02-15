package nifi.apex.examples.logs;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import org.apache.hadoop.conf.Configuration;

/**
 * Runs the LogLevelApplication in local mode.
 */
public class LogLevelApplicationRunner {

    public static void main(String[] args) throws Exception {
        StreamingApplication app = new LogLevelApplication();

        Configuration conf = new Configuration(false);
        conf.addResource(app.getClass().getResourceAsStream(
                "/META-INF/properties-LogLevelCount.xml"));

        LocalMode.runApp(app, conf, 140000);
    }

}
