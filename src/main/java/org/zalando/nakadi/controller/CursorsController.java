package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.CursorsService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.view.CursorCommitResult;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.springframework.http.HttpStatus.NOT_IMPLEMENTED;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.noContent;
import static org.springframework.http.ResponseEntity.ok;
import static org.springframework.http.ResponseEntity.status;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.HIGH_LEVEL_API;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class CursorsController {

    private static final Logger LOG = LoggerFactory.getLogger(CursorsController.class);

    private final CursorsService cursorsService;
    private final FeatureToggleService featureToggleService;
    private final CursorConverter cursorConverter;
    private final CursorTokenService cursorTokenService;

    @Autowired
    public CursorsController(final CursorsService cursorsService,
                             final FeatureToggleService featureToggleService,
                             final CursorConverter cursorConverter,
                             final CursorTokenService cursorTokenService) {
        this.cursorsService = cursorsService;
        this.featureToggleService = featureToggleService;
        this.cursorConverter = cursorConverter;
        this.cursorTokenService = cursorTokenService;
    }

    @RequestMapping(path = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.GET)
    public ResponseEntity<?> getCursors(@PathVariable("subscriptionId") final String subscriptionId,
                                        final NativeWebRequest request,
                                        final Client client) {
        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }
        try {
            final List<SubscriptionCursor> cursors = cursorsService.getSubscriptionCursors(subscriptionId, client)
                    .stream()
                    .map(cursor -> cursorConverter.convert(cursor, cursorTokenService.generateToken()))
                    .collect(Collectors.toList());
            return status(OK).body(new ItemsWrapper<>(cursors));
        } catch (final NakadiException e) {
            return create(e.asProblem(), request);
        }
    }

    @RequestMapping(value = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.POST)
    public ResponseEntity<?> commitCursors(@PathVariable("subscriptionId") final String subscriptionId,
                                           @Valid @RequestBody final ItemsWrapper<SubscriptionCursor> cursors,
                                           final Errors errors,
                                           @NotNull @RequestHeader("X-Nakadi-StreamId") final String streamId,
                                           final NativeWebRequest request,
                                           final Client client) {

        LOG.debug("[COMMIT_CURSORS] committing {} cursor(s) for subscription {}", cursors.getItems().size(),
                subscriptionId);

        if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
            return new ResponseEntity<>(NOT_IMPLEMENTED);
        }
        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        try {
            LOG.debug("[COMMIT_CURSORS] scopes validation finished");

            final List<Boolean> items = cursorsService
                    .commitCursors(streamId, subscriptionId, convertToNakadiCursors(cursors), client);

            LOG.debug("[COMMIT_CURSORS] commit finished");

            final boolean allCommited = items.stream().allMatch(item -> item);
            if (allCommited) {
                return noContent().build();
            } else {
                final List<CursorCommitResult> body = IntStream.range(0, cursors.getItems().size())
                        .mapToObj(idx -> new CursorCommitResult(cursors.getItems().get(idx), items.get(idx)))
                        .collect(Collectors.toList());
                return ok(new ItemsWrapper<>(body));
            }
        } catch (final NoSuchEventTypeException | InvalidCursorException e) {
            return create(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()), request);
        } catch (final NakadiException e) {
            return create(e.asProblem(), request);
        }
    }

    @RequestMapping(value = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.PATCH)
    public ResponseEntity<?> resetCursors(
            @PathVariable("subscriptionId") final String subscriptionId,
            @Valid @RequestBody final ItemsWrapper<SubscriptionCursorWithoutToken> cursors,
            final Errors errors,
            final NativeWebRequest request,
            final Client client) {
        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        try {
            cursorsService.resetCursors(subscriptionId, convertToNakadiCursors(cursors), client);
            return noContent().build();
        } catch (final InvalidCursorException e) {
            return create(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()), request);
        } catch (final NakadiException e) {
            return create(e.asProblem(), request);
        }
    }

    private List<NakadiCursor> convertToNakadiCursors(
            @Valid @RequestBody final ItemsWrapper<? extends SubscriptionCursorWithoutToken> cursors) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException, InvalidCursorException {
        final List<NakadiCursor> nakadiCursors = new ArrayList<>();
        for (final SubscriptionCursorWithoutToken cursor : cursors.getItems()) {
            nakadiCursors.add(cursorConverter.convert(cursor));
        }
        return nakadiCursors;
    }

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> handleUnableProcessException(final UnableProcessException ex,
                                                                final NativeWebRequest request) {
        LOG.debug(ex.getMessage(), ex);
        return Responses.create(MoreStatus.UNPROCESSABLE_ENTITY, ex.getMessage(), request);
    }

}
