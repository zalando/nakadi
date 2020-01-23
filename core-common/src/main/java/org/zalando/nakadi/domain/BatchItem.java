package org.zalando.nakadi.domain;

import javax.annotation.Nullable;
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
    private final EmptyInjectionConfiguration emptyInjectionConfiguration;
    private final InjectionConfiguration[] injections;
    private String[] injectionValues;
    private final List<Integer> skipCharacters;
    private String partition;
    private String brokerId;
    private Event event;
    private String key;
    private Optional<EventOwnerHeader> header = Optional.empty();

    public BatchItem(
            final String rawEvent,
            final EmptyInjectionConfiguration emptyInjectionConfiguration,
            final InjectionConfiguration[] injections,
            final List<Integer> skipCharacters) {
        this.skipCharacters = skipCharacters;
        this.emptyInjectionConfiguration = emptyInjectionConfiguration;
        this.injections = injections;
        this.response = new BatchItemResponse();
        this.event = new Event(rawEvent);
        Optional.ofNullable(this.event.getEventJson().optJSONObject("metadata"))
                .map(e -> e.optString("eid", null))
                .ifPresent(eid -> {
                    this.response.setEid(eid);
                    this.event.setId(eid);
                });
    }

    public void inject(final Injection type, final String value) {
        if (null == injectionValues) {
            injectionValues = new String[Injection.values().length];
        }
        injectionValues[type.ordinal()] = value;
    }

    public Event getEvent() {
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

    public Optional<EventOwnerHeader> getHeader() {
        return header;
    }

    @Nullable
    public String getKey() {
        return key;
    }

    public void setHeader(final EventOwnerHeader header) {
        this.header = Optional.ofNullable(header);
    }

    public void setKey(final String key) {
        this.key = key;
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

    public String dumpEventToString() {
        if (null == injectionValues) {
            if (skipCharacters.isEmpty()) {
                return event.getEventString();
            } else {
                final StringBuilder sb = new StringBuilder();
                appendWithSkip(sb, 0, event.getEventString().length(), 0);
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
        if (lastMainEventUsedPosition < event.getEventString().length()) {
            appendWithSkip(sb, lastMainEventUsedPosition, event.getEventString().length(), currentSkipPosition);
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
                sb.append(event.getEventString(), currentPos, currentSkipIdx);
            }
            currentPos = currentSkipIdx + 1;
        }
        if (to > currentPos) {
            sb.append(event.getEventString(), currentPos, to);
        }
        return idx;
    }

}
