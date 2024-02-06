package org.zalando.nakadi;

import org.zalando.nakadi.domain.HeaderTag;
import org.zalando.nakadi.exceptions.runtime.InvalidPublishingParamException;
import org.zalando.nakadi.security.Client;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class PublishRequest<T> {
    private final String eventTypeName;
    private final T eventsRaw;
    private final Client client;
    private final Map<HeaderTag, String> consumerTags;
    private final Optional<Integer> desiredPublishingTimeout;
    private final boolean isDeleteRequest;

    public PublishRequest(final String eventTypeName,
                          final T eventsRaw,
                          final Client client,
                          final Map<HeaderTag, String> consumerTags,
                          final int desiredPublishingTimeout,
                          final boolean isDeleteRequest) {
        this.eventTypeName = eventTypeName;
        this.eventsRaw = eventsRaw;
        this.client = client;
        this.consumerTags = consumerTags;
        //TODO: better way to get max timeout instead of hardcoding
        if (desiredPublishingTimeout < 0 || desiredPublishingTimeout > 30_000) {
            throw new InvalidPublishingParamException("X-TIMEOUT cannot be less than 0 or greater than 30000 ms");
        }
        //0 means either nothing was supplied or 0 was supplied, in both cases it means we will leave
        //the timeout to be current default
        this.desiredPublishingTimeout = Optional.of(desiredPublishingTimeout).filter(v -> v != 0);
        this.isDeleteRequest = isDeleteRequest;
    }

    public String getEventTypeName() {
        return eventTypeName;
    }

    public T getEventsRaw() {
        return eventsRaw;
    }

    public Client getClient() {
        return client;
    }

    public Map<HeaderTag, String> getConsumerTags() {
        return consumerTags;
    }

    public Optional<Integer> getDesiredPublishingTimeout() {
        return desiredPublishingTimeout;
    }

    public boolean isDeleteRequest() {
        return isDeleteRequest;
    }

    @Override
    public String toString() {
        return "PublishRequest{" +
                "eventTypeName='" + eventTypeName + '\'' +
                ", eventsAsString='" + eventsRaw + '\'' +
                ", client=" + client +
                ", consumerTags=" + consumerTags +
                ", desiredPublishingTimeout=" + desiredPublishingTimeout +
                ", isDeleteRequest=" + isDeleteRequest +
                '}';
    }

    public static <T> PublishRequest<T> asPublish(final String eventTypeName,
                                                  final T eventsRaw,
                                                  final Client client,
                                                  final Map<HeaderTag, String> consumerTags,
                                                  final int desiredPublishingTimeout) {
        return new PublishRequest<>(eventTypeName, eventsRaw, client,
                consumerTags, desiredPublishingTimeout, false);
    }

    public static <T> PublishRequest<T> asDelete(final String eventTypeName,
                                                 final T eventsRaw,
                                                 final Client client) {
        return new PublishRequest<>(eventTypeName, eventsRaw, client,
                Collections.emptyMap(), 0, true);
    }

}
