package nifi.flink.examples.logs.data;

import java.util.HashMap;
import java.util.Map;

/**
 * A map of log levels.
 */
public class LogLevels {

    private Map<String,Integer> levels = new HashMap<>();

    public void add(LogLevel logLevel) {
        int count = logLevel.getCount();

        if (levels.containsKey(logLevel.getLevel())) {
            count += levels.get(logLevel.getLevel());
        }

        levels.put(logLevel.getLevel(), count);
    }

    public Map<String, Integer> getLevels() {
        return levels;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("LOG LEVEL COUNTS {");

        for (Map.Entry<String,Integer> entry : levels.entrySet()) {
            builder.append("\nLEVEL = ")
                    .append(entry.getKey())
                    .append(", COUNT = ")
                    .append(entry.getValue());
        }

        builder.append("\n}");
        return builder.toString();
    }
}
