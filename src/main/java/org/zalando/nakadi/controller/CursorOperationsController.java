package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorDistanceQuery;
import org.zalando.nakadi.domain.NakadiCursorDistanceResult;
import org.zalando.nakadi.domain.NakadiCursorLag;
import org.zalando.nakadi.domain.ShiftedNakadiCursor;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.CursorDistanceQuery;
import org.zalando.nakadi.view.CursorDistanceResult;
import org.zalando.nakadi.view.CursorLag;
import org.zalando.nakadi.view.ShiftedCursor;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.validation.Valid;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;

@RestController
public class CursorOperationsController {

    private static final Logger LOG = LoggerFactory.getLogger(CursorOperationsController.class);

    private final CursorConverter cursorConverter;
    private final CursorOperationsService cursorOperationsService;

    @Autowired
    public CursorOperationsController(final CursorOperationsService cursorOperationsService,
                             final CursorConverter cursorConverter) {
        this.cursorOperationsService = cursorOperationsService;
        this.cursorConverter = cursorConverter;
    }

    @RequestMapping(path = "/event-types/{eventTypeName}/cursor-distances", method = RequestMethod.POST)
    public ResponseEntity<?> getDistance(@PathVariable("eventTypeName") final String eventTypeName,
                                         @Valid @RequestBody final List<CursorDistanceQuery> queries,
                                         final Errors errors,
                                         final NativeWebRequest request) {

        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        final List<NakadiCursorDistanceQuery> domainQueries = queries.stream()
                .map(this.toCursorDistanceQuery(eventTypeName))
                .collect(Collectors.toList());

        final List<NakadiCursorDistanceResult> domainResult = cursorOperationsService.calculateDistance(domainQueries);

        final List<CursorDistanceResult> viewResult = domainResult.stream().map(this::toCursorDistanceResult)
                .collect(Collectors.toList());

        return status(OK).body(viewResult);
    }

    @RequestMapping(path = "/event-types/{eventTypeName}/shifted-cursors", method = RequestMethod.POST)
    public ResponseEntity<?> moveCursors(@PathVariable("eventTypeName") final String eventTypeName,
                                         @Valid @RequestBody final List<ShiftedCursor> cursors,
                                         final Errors errors,
                                         final NativeWebRequest request) {

        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        final List<ShiftedNakadiCursor> domainCursor = cursors.stream()
                .map(this.toShiftedNakadiCursor(eventTypeName))
                .collect(Collectors.toList());

        final List<NakadiCursor> domainResultCursors = cursorOperationsService.unshiftCursors(domainCursor);

        final List<Cursor> viewResult = domainResultCursors.stream().map(cursorConverter::convert)
                .collect(Collectors.toList());

        return status(OK).body(viewResult);
    }

    @RequestMapping(path = "/event-types/{eventTypeName}/cursors-lag", method = RequestMethod.POST)
    public ResponseEntity<?> cursorsLag(@PathVariable("eventTypeName") final String eventTypeName,
                                         @Valid @RequestBody final List<Cursor> cursors,
                                         final Errors errors,
                                         final NativeWebRequest request) {

        if (errors.hasErrors()) {
            return Responses.create(new ValidationProblem(errors), request);
        }

        final List<NakadiCursor> domainCursor = cursors.stream()
                .map(toNakadiCursor(eventTypeName))
                .collect(Collectors.toList());

        final List<NakadiCursorLag> lagResult = cursorOperationsService
                .cursorsLag(eventTypeName, domainCursor);

        final List<CursorLag> viewResult = lagResult.stream().map(this::toCursorLag)
                .collect(Collectors.toList());

        return status(OK).body(viewResult);
    }

    @ExceptionHandler(InvalidCursorOperation.class)
    public ResponseEntity<Problem> invalidCursorOperation(final InvalidCursorOperation e,
                                                          final NativeWebRequest request) {
        return Responses.create(Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY,
                clientErrorMessage(e.getReason())), request);
    }

    private String clientErrorMessage(final InvalidCursorOperation.Reason reason) {
        switch (reason) {
            case INVERTED_TIMELINE_ORDER: return "Inverted timelines. Final cursor must correspond to a newer timeline " +
                    "than initial cursor.";
            case TIMELINE_NOT_FOUND: return "Timeline not found. It might happen in case the cursor refers to a " +
                    "timeline that has already expired.";
            case INVERTED_OFFSET_ORDER: return "Inverted offsets. Final cursor offsets must be newer than initial " +
                    "cursor offsets";
            case PARTITION_NOT_FOUND: return "Partition not found.";
            case CURSORS_WITH_DIFFERENT_PARTITION: return "Cursors with different partition. Pairs of cursors should " +
                    "have matching partitions.";
            default: {
                LOG.error("Unexpected invalid cursor operation reason " + reason);
                throw new MyNakadiRuntimeException1();
            }
        }
    }

    private CursorLag toCursorLag(final NakadiCursorLag nakadiCursorLag) {
        return new CursorLag(
                nakadiCursorLag.getPartition(),
                cursorConverter.convert(nakadiCursorLag.getFirstCursor()).getOffset(),
                cursorConverter.convert(nakadiCursorLag.getLastCursor()).getOffset(),
                nakadiCursorLag.getLag()
        );
    }

    private Function<Cursor, NakadiCursor> toNakadiCursor(final String eventTypeName) {
        return cursor -> {
            try {
                return cursorConverter.convert(eventTypeName, cursor);
            } catch (final NakadiException | InvalidCursorException e) {
                throw new MyNakadiRuntimeException1("problem converting cursors", e);
            }
        };
    }

    private Function<CursorDistanceQuery, NakadiCursorDistanceQuery> toCursorDistanceQuery(final String eventTypeName) {
        return (query) -> {
            try {
                return new NakadiCursorDistanceQuery(
                        cursorConverter.convert(eventTypeName, query.getInitialCursor()),
                        cursorConverter.convert(eventTypeName, query.getFinalCursor())
                );
            } catch (final NakadiException | InvalidCursorException e) {
                throw new MyNakadiRuntimeException1("problem converting cursors", e);
            }
        };
    }

    private Function<ShiftedCursor, ShiftedNakadiCursor> toShiftedNakadiCursor(final String eventTypeName) {
        return (cursor) -> {
            try {
                final NakadiCursor nakadiCursor = cursorConverter.convert(eventTypeName, cursor);
                return new ShiftedNakadiCursor(nakadiCursor.getTimeline(), nakadiCursor.getPartition(),
                        nakadiCursor.getOffset(), cursor.getShift());
            } catch (final NakadiException | InvalidCursorException e) {
                throw new MyNakadiRuntimeException1("problem converting cursors", e);
            }
        };
    }

    private CursorDistanceResult toCursorDistanceResult(final NakadiCursorDistanceResult domainResult) {
        final CursorDistanceResult viewResult = new CursorDistanceResult();
        viewResult.setDistance(domainResult.getDistance());
        viewResult.setInitialCursor(cursorConverter.convert(domainResult.getQuery().getInitialCursor()));
        viewResult.setFinalCursor(cursorConverter.convert(domainResult.getQuery().getFinalCursor()));
        return viewResult;
    }
}
