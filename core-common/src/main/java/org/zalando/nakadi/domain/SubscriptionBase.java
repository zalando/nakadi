package org.zalando.nakadi.domain;

import com.google.common.collect.ImmutableList;
import org.zalando.nakadi.annotations.validation.AnnotationKey;
import org.zalando.nakadi.annotations.validation.AnnotationValue;
import org.zalando.nakadi.annotations.validation.DeadLetterValidAnnotations;
import org.zalando.nakadi.annotations.validation.LabelKey;
import org.zalando.nakadi.annotations.validation.LabelValue;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.newTreeSet;

public class SubscriptionBase {

    public enum InitialPosition {
        BEGIN,
        END,
        CURSORS
    }

    @NotNull
    @Size(min = 1, message = "must contain at least one character")
    private String owningApplication;

    @NotNull
    @Size(min = 1, message = "must contain at least one element")
    private Set<String> eventTypes;

    @NotNull
    @Size(min = 1, message = "must contain at least one character")
    private String consumerGroup = "default";

    @NotNull
    private InitialPosition readFrom = InitialPosition.END;

    @Valid
    private List<SubscriptionCursorWithoutToken> initialCursors = ImmutableList.of();

    @Nullable
    @Valid
    private SubscriptionAuthorization authorization;

    @Nullable
    @DeadLetterValidAnnotations
    private Map<
            @AnnotationKey String,
            @AnnotationValue String> annotations;

    @Nullable
    private Map<
            @LabelKey String,
            @LabelValue String> labels;

    public SubscriptionBase() {
    }

    public SubscriptionBase(final SubscriptionBase subscriptionBase) {
        this.setOwningApplication(subscriptionBase.getOwningApplication());
        this.setEventTypes(subscriptionBase.getEventTypes());
        this.setConsumerGroup(subscriptionBase.getConsumerGroup());
        this.setReadFrom(subscriptionBase.getReadFrom());
        this.setInitialCursors(subscriptionBase.getInitialCursors());
        this.setAuthorization(subscriptionBase.getAuthorization());
        this.setAnnotations(subscriptionBase.getAnnotations());
        this.setLabels(subscriptionBase.getLabels());
    }

    public SubscriptionAuthorization getAuthorization() {
        return authorization;
    }

    public void setAuthorization(final SubscriptionAuthorization authorization) {
        this.authorization = authorization;
    }

    public String getOwningApplication() {
        return owningApplication;
    }

    public void setOwningApplication(final String owningApplication) {
        this.owningApplication = owningApplication;
    }

    public Set<String> getEventTypes() {
        return Collections.unmodifiableSet(eventTypes);
    }

    public void setEventTypes(final Set<String> eventTypes) {
        this.eventTypes = newTreeSet(eventTypes);
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(final String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public InitialPosition getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(final InitialPosition readFrom) {
        this.readFrom = readFrom;
    }

    public List<SubscriptionCursorWithoutToken> getInitialCursors() {
        return Collections.unmodifiableList(initialCursors);
    }

    public void setInitialCursors(@Nullable final List<SubscriptionCursorWithoutToken> initialCursors) {
        this.initialCursors = Optional.ofNullable(initialCursors).orElse(ImmutableList.of());
    }

    @Nullable
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(@Nullable final Map<String, String> annotations) {
        this.annotations = annotations;
    }

    @Nullable
    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(@Nullable final Map<String, String> labels) {
        this.labels = labels;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SubscriptionBase that = (SubscriptionBase) o;
        return Objects.equals(owningApplication, that.owningApplication)
                && Objects.equals(eventTypes, that.eventTypes)
                && Objects.equals(consumerGroup, that.consumerGroup)
                && Objects.equals(readFrom, that.readFrom)
                && Objects.equals(authorization, that.authorization)
                && Objects.equals(initialCursors, that.initialCursors)
                && Objects.equals(annotations, that.annotations)
                && Objects.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(owningApplication, eventTypes, consumerGroup, readFrom, initialCursors, annotations, labels);
    }

    public Resource<SubscriptionBase> asBaseResource(final String id) {
        return new ResourceImpl<>(id, ResourceImpl.SUBSCRIPTION_RESOURCE, getAuthorization(), this);
    }
}
