package nifi.apex.examples.logs;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.nifi.NiFiDataPacketBuilder;
import com.datatorrent.contrib.nifi.NiFiSinglePortInputOperator;
import com.datatorrent.contrib.nifi.NiFiSinglePortOutputOperator;
import com.datatorrent.lib.util.WindowDataManager;
import nifi.apex.examples.logs.data.DictionaryBuilder;
import nifi.apex.examples.logs.data.LogLevels;
import nifi.apex.examples.logs.operators.LogLevelWindowCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.remote.client.SiteToSiteClient;

/**
 * Application that pulls logs from NiFi and calculates the rate of ERROR/WARN messages
 * over a time window and sends a new set of levels to collect based on the rate.
 */
@ApplicationAnnotation(name="LogLevelCount")
public class LogLevelApplication implements StreamingApplication {

    @Override
    public void populateDAG(DAG dag, Configuration configuration) {
        LogLevelProperties props = new LogLevelProperties(configuration);

        //dag.setAttribute(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS, props.getWindowMillis());

        // create the operator to receive data from NiFi
        WindowDataManager inManager = new WindowDataManager.NoopWindowDataManager();
        NiFiSinglePortInputOperator nifiInput = getNiFiInput(dag, props, inManager);

        // create the operator to count log levels over a window
        String attributName = props.getLogLevelAttribute();
        LogLevelWindowCount count = dag.addOperator("count", new LogLevelWindowCount(attributName));
        dag.setAttribute(count, Context.OperatorContext.APPLICATION_WINDOW_COUNT, props.getAppWindowCount());

        // create the operator to send data back to NiFi
        WindowDataManager outManager = new WindowDataManager.NoopWindowDataManager();
        NiFiSinglePortOutputOperator nifiOutput = getNiFiOutput(dag, props, outManager);

        // configure the dag to get nifi-in -> count -> nifi-out
        dag.addStream("nifi-in-count", nifiInput.outputPort, count.input);
        dag.addStream("count-nifi-out", count.output, nifiOutput.inputPort);
    }

    private NiFiSinglePortInputOperator getNiFiInput(DAG dag, LogLevelProperties props, WindowDataManager windowDataManager) {
        final SiteToSiteClient.Builder inputConfig = new SiteToSiteClient.Builder()
                .url(props.getNifiUrl())
                .portName(props.getNifiInputPort())
                .requestBatchCount(props.getNifiRequestBatch());

        return dag.addOperator("nifi-in", new NiFiSinglePortInputOperator(inputConfig, windowDataManager));
    }

    private NiFiSinglePortOutputOperator getNiFiOutput(DAG dag, LogLevelProperties props, WindowDataManager windowDataManager) {
        final SiteToSiteClient.Builder outputConfig = new SiteToSiteClient.Builder()
                .url(props.getNifiUrl())
                .portName(props.getNifiOutputPort());

        final int batchSize = 1;
        final NiFiDataPacketBuilder<LogLevels> dataPacketBuilder = new DictionaryBuilder(
                props.getWindowMillis(), props.getLogLevelThreshold());

        return dag.addOperator("nifi-out", new NiFiSinglePortOutputOperator(
                outputConfig, dataPacketBuilder, windowDataManager ,batchSize));
    }

}
