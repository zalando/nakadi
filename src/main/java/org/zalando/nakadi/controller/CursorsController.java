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
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.CursorsAreEmptyException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.InvalidStreamIdException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.RequestInProgressException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.CursorsService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.util.TimeLogger;
import org.zalando.nakadi.view.CursorCommitResult;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import org.zalando.problem.Problem;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.springframework.http.ResponseEntity.noContent;
import static org.springframework.http.ResponseEntity.ok;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.HIGH_LEVEL_API;
import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.NOT_IMPLEMENTED;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@RestController
public class CursorsController implements NakadiProblemHandling {

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
    public ItemsWrapper<SubscriptionCursor> getCursors(@PathVariable("subscriptionId") final String subscriptionId) {
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);
        try {
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
                                           final NativeWebRequest request) {

        TimeLogger.startMeasure(
                "COMMIT_CURSORS sid:" + subscriptionId + ", size=" + cursorsIn.getItems().size(),
                "isFeatureEnabled");
        try {
            featureToggleService.checkFeatureOn(HIGH_LEVEL_API);

            try {
                TimeLogger.addMeasure("convertToNakadiCursors");
                final List<NakadiCursor> cursors = convertToNakadiCursors(cursorsIn);
                if (cursors.isEmpty()) {
                    throw new CursorsAreEmptyException();
                }
                TimeLogger.addMeasure("callService");
                final List<Boolean> items = cursorsService.commitCursors(streamId, subscriptionId, cursors);

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
            } catch (final ServiceTemporarilyUnavailableException e) {
                LOG.error("Failed to commit cursors", e);
                return create(Problem.valueOf(SERVICE_UNAVAILABLE, e.getMessage()), request);
            } catch (final NoSuchSubscriptionException e) {
                LOG.error("Failed to commit cursors", e);
                return create(Problem.valueOf(NOT_FOUND, e.getMessage()), request);
            } catch (final InternalNakadiException e) {
                LOG.error("Failed to commit cursors", e);
                return create(Problem.valueOf(INTERNAL_SERVER_ERROR, e.getMessage()), request);
            }
        } finally {
            LOG.info(TimeLogger.finishMeasure());
        }
    }

    @RequestMapping(value = "/subscriptions/{subscriptionId}/cursors", method = RequestMethod.PATCH)
    public ResponseEntity<?> resetCursors(
            @PathVariable("subscriptionId") final String subscriptionId,
            @Valid @RequestBody final ItemsWrapper<SubscriptionCursorWithoutToken> cursors,
            final NativeWebRequest request) {
        featureToggleService.checkFeatureOn(HIGH_LEVEL_API);
        try {
            cursorsService.resetCursors(subscriptionId, convertToNakadiCursors(cursors));
            return noContent().build();
        } catch (final NoSuchEventTypeException e) {
            throw new UnableProcessException(e.getMessage());
        } catch (final InvalidCursorException e) {
            return create(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()), request);
        } catch (final InternalNakadiException e) {
            return create(Problem.valueOf(INTERNAL_SERVER_ERROR, e.getMessage()), request);
        }
    }

    @ExceptionHandler(FeatureNotAvailableException.class)
    public ResponseEntity<Problem> handleFeatureNotAllowedException(final FeatureNotAvailableException exception,
                                                           final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_IMPLEMENTED, "Feature is disabled"), request);
    }

    @ExceptionHandler(InvalidStreamIdException.class)
    public ResponseEntity<Problem> handleInvalidStreamIdException(final InvalidStreamIdException exception,
                                                                  final NativeWebRequest request) {
        LOG.warn("Stream id {} is not found: {}", exception.getStreamId(), exception.getMessage());
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }

    @Override
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Problem> handleMethodArgumentNotValid(final MethodArgumentNotValidException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(new ValidationProblem(exception.getBindingResult()), request);
    }

    @ExceptionHandler(NoSuchSubscriptionException.class)
    public ResponseEntity<Problem> handleNoSuchSubscriptionException(final NoSuchSubscriptionException exception,
                                                                     final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(RequestInProgressException.class)
    public ResponseEntity<Problem> handleRequestInProgressException(final RequestInProgressException exception,
                                                                    final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(CONFLICT, exception.getMessage()), request);
    }

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> handleUnableProcessException(final RuntimeException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
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
