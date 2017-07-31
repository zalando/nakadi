package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
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
import org.zalando.nakadi.exceptions.InvalidStreamIdException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.RequestInProgressException;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.CursorsService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.util.TimeLogger;
import org.zalando.nakadi.view.CursorCommitResult;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);
        try {
            final List<SubscriptionCursor> cursors = cursorsService.getSubscriptionCursors(subscriptionId, client)
                    .stream()
                    .map(cursor -> cursor.withToken(cursorTokenService.generateToken()))
                    .collect(Collectors.toList());
            return status(OK).body(new ItemsWrapper<>(cursors));
        } catch (final NakadiException e) {
            return create(e.asProblem(), request);
        }
    }

    @RequestMapping(value = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.POST)
    public ResponseEntity<?> commitCursors(@PathVariable("subscriptionId") final String subscriptionId,
                                           @Valid @RequestBody final ItemsWrapper<SubscriptionCursor> cursorsIn,
                                           @NotNull @RequestHeader("X-Nakadi-StreamId") final String streamId,
                                           final NativeWebRequest request,
                                           final Client client) {

        TimeLogger.startMeasure(
                "COMMIT_CURSORS sid:" + subscriptionId + ", size=" + cursorsIn.getItems().size(),
                "isFeatureEnabled");
        try {
            featureToggleService.checkFeatureOn(HIGH_LEVEL_API);

            try {
                TimeLogger.addMeasure("convertToNakadiCursors");
                final List<NakadiCursor> cursors = convertToNakadiCursors(cursorsIn);

                TimeLogger.addMeasure("callService");
                final List<Boolean> items = cursorsService.commitCursors(streamId, subscriptionId, cursors, client);

                TimeLogger.addMeasure("prepareResponse");
                final boolean allCommited = items.stream().allMatch(item -> item);
                if (allCommited) {
                    return noContent().build();
                } else {
                    final List<CursorCommitResult> body = IntStream.range(0, cursorsIn.getItems().size())
                            .mapToObj(idx -> new CursorCommitResult(cursorsIn.getItems().get(idx), items.get(idx)))
                            .collect(Collectors.toList());
                    return ok(new ItemsWrapper<>(body));
                }
            } catch (final NoSuchEventTypeException | InvalidCursorException e) {
                return create(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()), request);
            } catch (final NakadiException e) {
                LOG.error("Failed to commit cursors", e);
                return create(e.asProblem(), request);
            }
        } finally {
            LOG.info(TimeLogger.finishMeasure());
        }
    }

    @RequestMapping(value = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.PATCH)
    public ResponseEntity<?> resetCursors(
            @PathVariable("subscriptionId") final String subscriptionId,
            @Valid @RequestBody final ItemsWrapper<SubscriptionCursorWithoutToken> cursors,
            final NativeWebRequest request,
            final Client client) {
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);

        try {
            cursorsService.resetCursors(subscriptionId, convertToNakadiCursors(cursors), client);
            return noContent().build();
        } catch (final NoSuchEventTypeException e) {
            throw new UnableProcessException(e.getMessage());
        } catch (final InvalidCursorException e) {
            return create(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()), request);
        } catch (final NakadiException e) {
            return create(e.asProblem(), request);
        }
    }

    private List<NakadiCursor> convertToNakadiCursors(
            final ItemsWrapper<? extends SubscriptionCursorWithoutToken> cursors) throws
            InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException, InvalidCursorException {
        final List<NakadiCursor> nakadiCursors = new ArrayList<>();
        for (final SubscriptionCursorWithoutToken cursor : cursors.getItems()) {
            nakadiCursors.add(cursorConverter.convert(cursor));
        }
        return nakadiCursors;
    }

    @ExceptionHandler(InvalidStreamIdException.class)
    public ResponseEntity<Problem> handleInvalidStreamId(final InvalidStreamIdException ex,
                                                         final NativeWebRequest request) {
        LOG.warn("Stream id {} is not found", ex.getStreamId(), ex);
        return Responses.create(MoreStatus.UNPROCESSABLE_ENTITY, ex.getMessage(), request);
    }

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> handleUnableProcessException(final UnableProcessException ex,
                                                                final NativeWebRequest request) {
        LOG.debug(ex.getMessage(), ex);
        return Responses.create(MoreStatus.UNPROCESSABLE_ENTITY, ex.getMessage(), request);
    }

    @ExceptionHandler(RequestInProgressException.class)
    public ResponseEntity<Problem> handleRequestInProgressException(final RequestInProgressException ex,
                                                                    final NativeWebRequest request) {
        LOG.debug(ex.getMessage(), ex);
        return Responses.create(Response.Status.CONFLICT, ex.getMessage(), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Problem> handleMethodArgumentNotValidException(final MethodArgumentNotValidException ex,
                                                                         final NativeWebRequest request) {
        LOG.debug(ex.getMessage(), ex);
        return Responses.create(new ValidationProblem(ex.getBindingResult()), request);
    }

    @ExceptionHandler(FeatureNotAvailableException.class)
    public ResponseEntity<Problem> handleFeatureNotAllowed(final FeatureNotAvailableException ex,
                                                           final NativeWebRequest request) {
        LOG.debug(ex.getMessage(), ex);
        return Responses.create(Problem.valueOf(Response.Status.NOT_IMPLEMENTED, "Feature is disabled"), request);
    }

}
