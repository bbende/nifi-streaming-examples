package nifi.storm.examples.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A Map of log levels.
 *
 * @author bbende
 */
public class LogLevels implements Serializable {

    private Map<String,Integer> levels = new HashMap<>();

    public void add(final String level, final int count) {
        int totalCount = count;
        if (levels.containsKey(level)) {
            totalCount += levels.get(level);
        }
        levels.put(level, totalCount);
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
