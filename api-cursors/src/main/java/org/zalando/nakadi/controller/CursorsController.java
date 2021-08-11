package org.zalando.nakadi.controller;

import io.opentracing.Tracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.runtime.BlockedException;
import org.zalando.nakadi.exceptions.runtime.CursorsAreEmptyException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.CursorsService;
import org.zalando.nakadi.service.EventStreamChecks;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.view.CursorCommitResult;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
public class CursorsController {

    private final CursorsService cursorsService;
    private final CursorConverter cursorConverter;
    private final CursorTokenService cursorTokenService;
    private final EventStreamChecks eventStreamChecks;

    @Autowired
    public CursorsController(final CursorsService cursorsService,
                             final CursorConverter cursorConverter,
                             final CursorTokenService cursorTokenService,
                             final EventStreamChecks eventStreamChecks) {
        this.cursorsService = cursorsService;
        this.cursorConverter = cursorConverter;
        this.cursorTokenService = cursorTokenService;
        this.eventStreamChecks = eventStreamChecks;
    }

    @RequestMapping(path = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.GET)
    public ItemsWrapper<SubscriptionCursor> getCursors(@PathVariable("subscriptionId") final String subscriptionId,
                                                       final Client client) {
        try {
            if (eventStreamChecks.isSubscriptionConsumptionBlocked(subscriptionId, client.getClientId())) {
                throw new BlockedException("Application or subscription is blocked");
            }
            final List<SubscriptionCursor> cursors = cursorsService.getSubscriptionCursors(subscriptionId)
                    .stream()
                    .map(cursor -> cursor.withToken(cursorTokenService.generateToken()))
                    .collect(Collectors.toList());
            return new ItemsWrapper<>(cursors);
        } catch (final InternalNakadiException e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @RequestMapping(value = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.POST)
    public ResponseEntity<?> commitCursors(@PathVariable("subscriptionId") final String subscriptionId,
                                           @Valid @RequestBody final ItemsWrapper<SubscriptionCursor> cursorsIn,
                                           @NotNull @RequestHeader("X-Nakadi-StreamId") final String streamId,
                                           final HttpServletRequest request,
                                           final Client client)
            throws NoSuchEventTypeException,
            NoSuchSubscriptionException,
            InvalidCursorException,
            ServiceTemporarilyUnavailableException,
            InternalNakadiException {
        final List<NakadiCursor> cursors = convertToNakadiCursors(cursorsIn);
        if (cursors.isEmpty()) {
            throw new CursorsAreEmptyException();
        }

        final Tracer.SpanBuilder commitSpan =
                TracingService.buildNewSpan("commit_events")
                .withTag("subscription.id", subscriptionId)
                .withTag("stream.id", streamId);
        try (Closeable ignored = TracingService.withActiveSpan(commitSpan)) {

            if (eventStreamChecks.isSubscriptionConsumptionBlocked(subscriptionId, client.getClientId())) {
                TracingService.logError("Application or subscription is blocked");
                throw new BlockedException("Application or subscription is blocked");
            }
            final List<Boolean> items = cursorsService.commitCursors(streamId, subscriptionId, cursors);

            final boolean allCommitted = items.stream().allMatch(item -> item);
            if (allCommitted) {
                return ResponseEntity.noContent().build();
            } else {
                final List<CursorCommitResult> body = IntStream.range(0, cursorsIn.getItems().size())
                    .mapToObj(idx -> new CursorCommitResult(cursorsIn.getItems().get(idx), items.get(idx)))
                    .collect(Collectors.toList());
                return ResponseEntity.ok(new ItemsWrapper<>(body));
            }
        } catch (final IOException ioe) {
            throw new InternalNakadiException("Error closing active span scope", ioe);
        }
    }

    @RequestMapping(value = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.PATCH)
    public ResponseEntity<?> resetCursors(
            @PathVariable("subscriptionId") final String subscriptionId,
            @Valid @RequestBody final ItemsWrapper<SubscriptionCursorWithoutToken> cursors,
            final HttpServletRequest request,
            final Client client)
            throws NoSuchEventTypeException, InvalidCursorException, InternalNakadiException {
        if (eventStreamChecks.isSubscriptionConsumptionBlocked(subscriptionId, client.getClientId())) {
            throw new BlockedException("Application or subscription is blocked");
        }

        final Tracer.SpanBuilder resetSpan =
                TracingService.buildNewSpan("reset_cursors")
                .withTag("subscription.id", subscriptionId);
        try (Closeable ignored = TracingService.withActiveSpan(resetSpan)) {
            cursorsService.resetCursors(subscriptionId, convertToNakadiCursors(cursors));
        } catch (final IOException ioe) {
            throw new InternalNakadiException("Error closing active span scope", ioe);
        }
        return ResponseEntity.noContent().build();
    }

    private List<NakadiCursor> convertToNakadiCursors(
            final ItemsWrapper<? extends SubscriptionCursorWithoutToken> cursors) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException,
            InvalidCursorException {
        final List<NakadiCursor> nakadiCursors = new ArrayList<>();
        for (final SubscriptionCursorWithoutToken cursor : cursors.getItems()) {
            nakadiCursors.add(cursorConverter.convert(cursor));
        }
        return nakadiCursors;
    }
}
