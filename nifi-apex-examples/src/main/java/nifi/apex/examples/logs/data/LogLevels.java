package nifi.apex.examples.logs.data;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper to hold a Map of log levels to their counts.
 */
public class LogLevels {

    private Map<String,Integer> levels = new HashMap<>();

    /**
     * @param level the level to add
     * @param count the count for the level to add
     */
    public void add(String level, int count) {
        if (levels.containsKey(level)) {
            count += levels.get(level);
        }

        levels.put(level, count);
    }

    public Integer get(String level) {
        if (levels.containsKey(level)) {
            return levels.get(level);
        } else {
            return 0;
        }
    }

    /**
     * @return true if there are no level counts, false otherwise
     */
    public boolean isEmpty() {
        return levels.isEmpty();
    }

    /**
     * Clears the current state of the level counts
     */
    public void clear() {
        levels.clear();
    }

    /**
     * @return a deep copy of the current instance
     */
    public LogLevels deepCopy() {
        LogLevels copy = new LogLevels();

        for (Map.Entry<String,Integer> entry : levels.entrySet()) {
            copy.add(entry.getKey(), entry.getValue());
        }

        return copy;
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
