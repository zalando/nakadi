package org.zalando.nakadi.util;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class TimeLogger {

    private static final ThreadLocal<List<LogCollector>> THREAD_COLLECTORS = new ThreadLocal<>();

    private static class LogCollector {
        private List<Long> stepStarts;
        private List<String> stepNames;

        LogCollector() {
            this.stepStarts = new ArrayList<>(20);
            this.stepNames = new ArrayList<>(this.stepStarts.size());
        }

        private void addRecord(final String message) {
            this.stepStarts.add(System.currentTimeMillis());
            this.stepNames.add(message);
        }

        private String describe() {
            final StringBuilder builder = new StringBuilder("[" + stepNames.get(0) + "]");
            for (int i = 1; i < stepStarts.size() - 1; ++i) {
                builder.append(" [")
                        .append(stepNames.get(i))
                        .append("=")
                        .append(stepStarts.get(i + 1) - stepStarts.get(i))
                        .append("]");
            }
            return builder.toString();
        }
    }

    public static void startMeasure(final String heading, final String stepName) {
        final LogCollector collector = new LogCollector();
        List<LogCollector> collectors = THREAD_COLLECTORS.get();
        if (null == collectors) {
            collectors = new ArrayList<>(2);
            THREAD_COLLECTORS.set(collectors);
        }
        if (!collectors.isEmpty()) {
            collectors.get(collectors.size() - 1).addRecord(heading);
        }
        collectors.add(collector);
        collector.addRecord(heading);
        collector.addRecord(stepName);
    }

    @Nullable
    public static String finishMeasure() {
        final List<LogCollector> threadCollectors = THREAD_COLLECTORS.get();
        if (null != threadCollectors && !threadCollectors.isEmpty()) {
            final LogCollector lastCollector = threadCollectors.remove(threadCollectors.size() - 1);
            lastCollector.addRecord("finish");
            return lastCollector.describe();
        }
        return null;
    }

    public static void addMeasure(final String message) {
        final List<LogCollector> collectors = THREAD_COLLECTORS.get();
        if (null != collectors && !collectors.isEmpty()) {
            collectors.get(collectors.size() - 1).addRecord(message);
        }
    }

    private TimeLogger() {
    }
}
