package org.zalando.nakadi.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class NakadiMetricsModule extends Module {
    static final Version VERSION = new Version(3, 1, 3, "", "com.codahale.metrics", "metrics-json");

    private static class GaugeSerializer extends StdSerializer<Gauge> {
        private GaugeSerializer() {
            super(Gauge.class);
        }

        @Override
        public void serialize(final Gauge gauge,
                              final JsonGenerator json,
                              final SerializerProvider provider) throws IOException {
            json.writeStartObject();
            final Object value;
            try {
                value = gauge.getValue();
                json.writeObjectField("value", value);
            } catch (RuntimeException e) {
                json.writeObjectField("error", e.toString());
            }
            json.writeEndObject();
        }
    }

    private static class CounterSerializer extends StdSerializer<Counter> {
        private CounterSerializer() {
            super(Counter.class);
        }

        @Override
        public void serialize(final Counter counter,
                              final JsonGenerator json,
                              final SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeNumberField("count", counter.getCount());
            json.writeEndObject();
        }
    }

    private static class HistogramSerializer extends StdSerializer<Histogram> {
        private final boolean showSamples;

        private HistogramSerializer(final boolean showSamples) {
            super(Histogram.class);
            this.showSamples = showSamples;
        }

        @Override
        public void serialize(final Histogram histogram,
                              final JsonGenerator json,
                              final SerializerProvider provider) throws IOException {
            json.writeStartObject();
            final Snapshot snapshot = histogram.getSnapshot();
            json.writeNumberField("count", histogram.getCount());
            json.writeNumberField("mean", snapshot.getMean());

            if (showSamples) {
                json.writeObjectField("values", snapshot.getValues());
            }

            json.writeEndObject();
        }
    }

    private static class MeterSerializer extends StdSerializer<Meter> {
        private final String rateUnit;
        private final double rateFactor;

        MeterSerializer(final TimeUnit rateUnit) {
            super(Meter.class);
            this.rateFactor = rateUnit.toSeconds(1);
            this.rateUnit = calculateRateUnit(rateUnit, "events");
        }

        @Override
        public void serialize(final Meter meter,
                              final JsonGenerator json,
                              final SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeNumberField("count", meter.getCount());
            json.writeNumberField("m1_rate", meter.getOneMinuteRate() * rateFactor);
            json.writeStringField("units", rateUnit);
            json.writeEndObject();
        }
    }

    private static class TimerSerializer extends StdSerializer<Timer> {
        private final String rateUnit;
        private final double rateFactor;
        private final String durationUnit;
        private final double durationFactor;
        private final boolean showSamples;

        private TimerSerializer(final TimeUnit rateUnit,
                                final TimeUnit durationUnit,
                                final boolean showSamples) {
            super(Timer.class);
            this.rateUnit = calculateRateUnit(rateUnit, "calls");
            this.rateFactor = rateUnit.toSeconds(1);
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
            this.durationFactor = 1.0 / durationUnit.toNanos(1);
            this.showSamples = showSamples;
        }

        @Override
        public void serialize(final Timer timer,
                              final JsonGenerator json,
                              final SerializerProvider provider) throws IOException {
            json.writeStartObject();
            final Snapshot snapshot = timer.getSnapshot();
            json.writeNumberField("count", timer.getCount());
            json.writeNumberField("mean", snapshot.getMean() * durationFactor);

            if (showSamples) {
                final long[] values = snapshot.getValues();
                final double[] scaledValues = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    scaledValues[i] = values[i] * durationFactor;
                }
                json.writeObjectField("values", scaledValues);
            }

            json.writeNumberField("m1_rate", timer.getOneMinuteRate() * rateFactor);
            json.writeStringField("duration_units", durationUnit);
            json.writeStringField("rate_units", rateUnit);
            json.writeEndObject();
        }
    }

    private static class MetricRegistrySerializer extends StdSerializer<MetricRegistry> {

        private final MetricFilter filter;

        private MetricRegistrySerializer(final MetricFilter filter) {
            super(MetricRegistry.class);
            this.filter = filter;
        }

        @Override
        public void serialize(final MetricRegistry registry,
                              final JsonGenerator json,
                              final SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeStringField("version", VERSION.toString());
            json.writeObjectField("gauges", registry.getGauges(filter));
            json.writeObjectField("counters", registry.getCounters(filter));
            json.writeObjectField("histograms", registry.getHistograms(filter));
            json.writeObjectField("meters", registry.getMeters(filter));
            json.writeObjectField("timers", registry.getTimers(filter));
            json.writeEndObject();
        }
    }

    private final TimeUnit rateUnit;
    private final TimeUnit durationUnit;
    private final boolean showSamples;
    private final MetricFilter filter;

    public NakadiMetricsModule(final TimeUnit rateUnit,
                               final TimeUnit durationUnit,
                               final boolean showSamples) {
        this(rateUnit, durationUnit, showSamples, MetricFilter.ALL);
    }

    public NakadiMetricsModule(final TimeUnit rateUnit,
                               final TimeUnit durationUnit,
                               final boolean showSamples,
                               final MetricFilter filter) {
        this.rateUnit = rateUnit;
        this.durationUnit = durationUnit;
        this.showSamples = showSamples;
        this.filter = filter;
    }

    @Override
    public String getModuleName() {
        return "metrics";
    }

    @Override
    public Version version() {
        return VERSION;
    }

    @Override
    public void setupModule(final SetupContext context) {
        context.addSerializers(new SimpleSerializers(Arrays.<JsonSerializer<?>>asList(
                new GaugeSerializer(),
                new CounterSerializer(),
                new HistogramSerializer(showSamples),
                new MeterSerializer(rateUnit),
                new TimerSerializer(rateUnit, durationUnit, showSamples),
                new MetricRegistrySerializer(filter)
        )));
    }

    private static String calculateRateUnit(final TimeUnit unit, final String name) {
        final String s = unit.toString().toLowerCase(Locale.US);
        return name + '/' + s.substring(0, s.length() - 1);
    }
}