package org.zalando.nakadi.domain;

import org.json.JSONObject;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchItem {

    public enum Injection {
        METADATA("metadata"),;
        public final String name;

        Injection(final String name) {
            this.name = name;
        }
    }

    public static class InjectionConfiguration {
        private final int startPos;
        private final int endPos;

        public InjectionConfiguration(final int startPos, final int endPos) {
            this.startPos = startPos;
            this.endPos = endPos;
        }
    }

    public static class EmptyInjectionConfiguration {
        private final int position;
        private final boolean addComma;

        public EmptyInjectionConfiguration(final int position, final boolean addComma) {
            this.position = position;
            this.addComma = addComma;
        }
        public static EmptyInjectionConfiguration build(final int position, final boolean addComma) {
            if (position == 1) {
                return addComma ? CONFIG_COMMA : CONFIG_NO_COMMA;
            }
            return new EmptyInjectionConfiguration(position, addComma);
        }
    }
    private static final EmptyInjectionConfiguration CONFIG_COMMA = new EmptyInjectionConfiguration(1, true);
    private static final EmptyInjectionConfiguration CONFIG_NO_COMMA = new EmptyInjectionConfiguration(1, false);

    private final BatchItemResponse response;
    private final String rawEvent;
    private final JSONObject event;
    private final EmptyInjectionConfiguration emptyInjectionConfiguration;
    private final Map<Injection, InjectionConfiguration> injections;
    private Map<Injection, String> injectionValues;
    private final List<Integer> skipCharacters;
    private String partition;
    private String brokerId;
    private int eventSize;

    public BatchItem(
            final String event,
            final EmptyInjectionConfiguration emptyInjectionConfiguration,
            final Map<Injection, InjectionConfiguration> injections,
            final List<Integer> skipCharacters) {
        this.rawEvent = event;
        this.skipCharacters = skipCharacters;
        this.event = new JSONObject(event);
        this.eventSize = event.getBytes(StandardCharsets.UTF_8).length;
        this.emptyInjectionConfiguration = emptyInjectionConfiguration;
        this.injections = injections;
        this.response = new BatchItemResponse();

        Optional.ofNullable(this.event.optJSONObject("metadata"))
                .map(e -> e.optString("eid", null))
                .ifPresent(this.response::setEid);
    }

    public void inject(final Injection type, final String value) {
        if (null == injectionValues) {
            injectionValues = new HashMap<>();
        }
        injectionValues.put(type, value);
    }

    public JSONObject getEvent() {
        return this.event;
    }

    public void setPartition(final String partition) {
        this.partition = partition;
    }

    @Nullable
    public String getPartition() {
        return partition;
    }

    @Nullable
    public String getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(final String brokerId) {
        this.brokerId = brokerId;
    }

    public BatchItemResponse getResponse() {
        return response;
    }

    public void setStep(final EventPublishingStep step) {
        response.setStep(step);
    }

    public EventPublishingStep getStep() {
        return response.getStep();
    }

    public void updateStatusAndDetail(final EventPublishingStatus publishingStatus, final String detail) {
        response.setPublishingStatus(publishingStatus);
        response.setDetail(detail);
    }

    public int getEventSize() {
        return eventSize;
    }

    public String dumpEventToString() {
        if (null == injectionValues || injectionValues.isEmpty()) {
            if (skipCharacters.isEmpty()) {
                return rawEvent;
            } else {
                final StringBuilder sb = new StringBuilder();
                appendWithSkip(sb, 0, rawEvent.length(), 0);
                return sb.toString();
            }
        }
        final AtomicBoolean nonComaAdded = new AtomicBoolean();
        final AtomicInteger lastMainEventUsedPosition = new AtomicInteger(0);
        final AtomicInteger currentSkipPosition = new AtomicInteger(0);
        final StringBuilder sb = new StringBuilder();

        injectionValues.entrySet().stream().sorted(Comparator.comparing(v -> {
            final InjectionConfiguration config = injections.get(v.getKey());
            if (config == null) {
                return emptyInjectionConfiguration.position;
            }
            return config.startPos;
        })).forEach(entry -> {
            final InjectionConfiguration config = injections.get(entry.getKey());
            final int positionStart;
            final int positionEnd;
            if (null != config) {
                positionStart = config.startPos;
                positionEnd = config.endPos;
            } else {
                positionStart = this.emptyInjectionConfiguration.position;
                positionEnd = this.emptyInjectionConfiguration.position;
            }

            if (positionStart > lastMainEventUsedPosition.get()) {
                currentSkipPosition.set(
                        appendWithSkip(sb, lastMainEventUsedPosition.get(), positionStart, currentSkipPosition.get()));
                sb.append(this.rawEvent, lastMainEventUsedPosition.get(), positionStart);
                lastMainEventUsedPosition.set(positionEnd);
            }
            sb.append(entry.getValue());
            if (!emptyInjectionConfiguration.addComma) {
                // Well, really rare case, but we are trying to load brain, so cover it as well
                if (nonComaAdded.get()) {
                    sb.append(",");
                }
                nonComaAdded.set(true);
                sb.append(entry.getValue());
            }
        });
        if (lastMainEventUsedPosition.get() < rawEvent.length()) {
            appendWithSkip(sb, lastMainEventUsedPosition.get(), rawEvent.length(), currentSkipPosition.get());
        }
        return sb.toString();
    }

    private int appendWithSkip(final StringBuilder sb, final int from, final int to, final int currentSkipPosition) {
        int currentPos = from;
        int idx;
        for (idx = currentSkipPosition; idx < skipCharacters.size(); ++idx) {
            final int currentSkipIdx = skipCharacters.get(idx);
            if (currentSkipIdx > to) {
                break;
            }
            if ((currentSkipIdx - currentPos) > 1) {
                sb.append(rawEvent, currentPos, currentSkipIdx);
            }
            currentPos = currentSkipIdx + 1;
        }
        if ((to - currentPos) > 1) {
            sb.append(rawEvent, currentPos, to);
        }
        return idx;
    }

}