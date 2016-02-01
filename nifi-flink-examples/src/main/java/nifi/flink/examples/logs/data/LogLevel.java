package nifi.flink.examples.logs.data;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

/**
 * A wrapper for a log level (i.e. warn, error, info) and a count for the level.
 */
public final class LogLevel extends Tuple2<String,Integer> implements Serializable {

    public LogLevel() {

    }

    public LogLevel(String level, int count) {
        super(level, count);
    }

    public String getLevel() {
        return getField(0);
    }

    public int getCount() {
        return getField(1);
    }

    @Override
    public String toString() {
        return "[ LEVEL = " + getLevel() + ", COUNT = " + getCount() + " ]";
    }
}
