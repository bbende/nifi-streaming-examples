package nifi.storm.examples.bolt;

import nifi.storm.examples.data.LogLevels;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

/**
 * A windowed bolt that takes incoming tuples containing a log level, and produces map containing
 * the total of each level with in the window.
 *
 * @author bbende
 */
public class LogLevelWindowBolt extends BaseWindowedBolt {

    private final String logLevelAttribute;
    private OutputCollector collector;

    /**
     * @param logLevelAttribute the name of the field in the tuple containing the log level
     */
    public LogLevelWindowBolt(String logLevelAttribute) {
        this.logLevelAttribute = logLevelAttribute;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }


    @Override
    public void execute(TupleWindow tupleWindow) {
        final LogLevels logLevels = new LogLevels();

        // for each tuple get the log level and update the count in the levels map
        for(Tuple tuple: tupleWindow.get()) {
            if (tuple.contains(logLevelAttribute)) {
                final String logLevel = tuple.getStringByField(logLevelAttribute);
                logLevels.add(logLevel, 1);
            }
        }

        // emit the whole map of counts as the results
        collector.emit(new Values(logLevels));

        // ack all the tuples, should we do this in the above loop??
        for(Tuple tuple: tupleWindow.get()) {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("counts"));
    }

}
