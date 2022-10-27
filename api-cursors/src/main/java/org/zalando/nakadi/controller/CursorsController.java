package org.zalando.nakadi.controller;

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
import org.zalando.nakadi.util.MDCUtils;
import org.zalando.nakadi.view.CursorCommitResult;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
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
        try (MDCUtils.CloseableNoEx ignore = MDCUtils.withSubscriptionId(subscriptionId)) {
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
                                           final Client client)
            throws NoSuchEventTypeException,
            NoSuchSubscriptionException,
            InvalidCursorException,
            ServiceTemporarilyUnavailableException,
            InternalNakadiException {

        try (MDCUtils.CloseableNoEx ignore = MDCUtils.withSubscriptionIdStreamId(subscriptionId, streamId)) {
            TracingService.setOperationName("commit_cursors")
                    .setTag("subscription.id", subscriptionId)
                    .setTag("stream.id", streamId);
            final List<NakadiCursor> cursors = convertToNakadiCursors(cursorsIn);
            if (cursors.isEmpty()) {
                throw new CursorsAreEmptyException();
            }

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
        }
    }

    @RequestMapping(value = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.PATCH)
    public ResponseEntity<?> resetCursors(
            @PathVariable("subscriptionId") final String subscriptionId,
            @Valid @RequestBody final ItemsWrapper<SubscriptionCursorWithoutToken> cursors,
            final Client client)
            throws NoSuchEventTypeException, InvalidCursorException, InternalNakadiException {
        try (MDCUtils.CloseableNoEx ignore = MDCUtils.withSubscriptionId(subscriptionId)) {

            TracingService.setOperationName("reset_cursors")
                    .setTag("subscription.id", subscriptionId);

            if (eventStreamChecks.isSubscriptionConsumptionBlocked(subscriptionId, client.getClientId())) {
                throw new BlockedException("Application or subscription is blocked");
            }

            cursorsService.resetCursors(subscriptionId, convertToNakadiCursors(cursors));
            return ResponseEntity.noContent().build();
        }
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
