package nifi.flink.examples.logs.functions;

import nifi.flink.examples.logs.data.LogLevel;
import nifi.flink.examples.logs.data.LogLevels;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Combines incoming LogLevels to a single object with total counts for each level.
 */
public final class LogLevelWindowCounter implements AllWindowFunction<LogLevel, LogLevels, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<LogLevel> iterable, Collector<LogLevels> collector)
            throws Exception {

        LogLevels levels = new LogLevels();
        for (LogLevel logLevel : iterable) {
            levels.add(logLevel);
        }

        collector.collect(levels);
    }
}
