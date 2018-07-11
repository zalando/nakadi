package org.zalando.nakadi.domain;

import org.json.JSONObject;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class BatchItem {

    public enum Injection {
        METADATA("metadata");
        public final String name;

        Injection(final String name) {
            this.name = name;
        }
    }

    public static class InjectionConfiguration {
        public final Injection injection;
        private final int startPos;
        private final int endPos;

        public InjectionConfiguration(final Injection injection, final int startPos, final int endPos) {
            this.injection = injection;
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
    private final InjectionConfiguration[] injections;
    private String[] injectionValues;
    private final List<Integer> skipCharacters;
    private String partition;
    private String brokerId;
    private String eventKey;
    private int eventSize;

    public BatchItem(
            final String rawEvent,
            final EmptyInjectionConfiguration emptyInjectionConfiguration,
            final InjectionConfiguration[] injections,
            final List<Integer> skipCharacters) {
        this.rawEvent = rawEvent;
        this.skipCharacters = skipCharacters;
        this.event = StrictJsonParser.parseObject(rawEvent);
        this.eventSize = rawEvent.getBytes(StandardCharsets.UTF_8).length;
        this.emptyInjectionConfiguration = emptyInjectionConfiguration;
        this.injections = injections;
        this.response = new BatchItemResponse();

        Optional.ofNullable(this.event.optJSONObject("metadata"))
                .map(e -> e.optString("eid", null))
                .ifPresent(this.response::setEid);
    }

    public void inject(final Injection type, final String value) {
        if (null == injectionValues) {
            injectionValues = new String[Injection.values().length];
        }
        injectionValues[type.ordinal()] = value;
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
    public String getEventKey() {
        return eventKey;
    }

    public void setEventKey(@Nullable final String eventKey) {
        this.eventKey = eventKey;
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
        if (null == injectionValues) {
            if (skipCharacters.isEmpty()) {
                return rawEvent;
            } else {
                final StringBuilder sb = new StringBuilder();
                appendWithSkip(sb, 0, rawEvent.length(), 0);
                return sb.toString();
            }
        }
        boolean nonComaAdded = false;
        int lastMainEventUsedPosition = 0;
        int currentSkipPosition = 0;
        final StringBuilder sb = new StringBuilder();
        final Injection[] sortedInjections = Arrays.copyOf(Injection.values(), Injection.values().length);
        Arrays.sort(sortedInjections, Comparator.comparing(injection -> {
            final InjectionConfiguration config = injections[injection.ordinal()];
            return null == config ? emptyInjectionConfiguration.position : config.startPos;
        }));

        for (final Injection injectionKey : sortedInjections) {
            final String injectionValue = injectionValues[injectionKey.ordinal()];
            if (injectionValue == null) {
                continue;
            }
            final InjectionConfiguration config = injections[injectionKey.ordinal()];
            final int positionStart;
            final int positionEnd;
            if (null != config) {
                positionStart = config.startPos;
                positionEnd = config.endPos;
            } else {
                positionStart = this.emptyInjectionConfiguration.position;
                positionEnd = this.emptyInjectionConfiguration.position;
            }

            if (positionStart > lastMainEventUsedPosition) {
                currentSkipPosition = appendWithSkip(sb, lastMainEventUsedPosition, positionStart, currentSkipPosition);
                lastMainEventUsedPosition = positionEnd;
            }
            sb.append('\"').append(injectionKey.name).append("\":");
            sb.append(injectionValue);
            if (config == null) {
                if (!emptyInjectionConfiguration.addComma) {
                    // Well, really rare case, but we are trying to load brain, so cover it as well
                    if (nonComaAdded) {
                        sb.append(',');
                    } else {
                        nonComaAdded = true;
                    }
                } else {
                    sb.append(',');
                }
            }
        }
        if (lastMainEventUsedPosition < rawEvent.length()) {
            appendWithSkip(sb, lastMainEventUsedPosition, rawEvent.length(), currentSkipPosition);
        }
        return sb.toString();
    }

    private int appendWithSkip(final StringBuilder sb, final int from, final int to, final int currentSkipPosition) {
        int currentPos = from;
        int idx;
        for (idx = currentSkipPosition; idx < skipCharacters.size(); ++idx) {
            final int currentSkipIdx = skipCharacters.get(idx);
            if (currentSkipIdx < from) {
                continue;
            }
            if (currentSkipIdx > to) {
                break;
            }
            if (currentSkipIdx > currentPos) {
                sb.append(rawEvent, currentPos, currentSkipIdx);
            }
            currentPos = currentSkipIdx + 1;
        }
        if (to > currentPos) {
            sb.append(rawEvent, currentPos, to);
        }
        return idx;
    }

}
