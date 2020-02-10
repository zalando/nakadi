package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorLag;
import org.zalando.nakadi.domain.ShiftedNakadiCursor;
import org.zalando.nakadi.exceptions.runtime.CursorConversionException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.util.ValidListWrapper;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.CursorDistance;
import org.zalando.nakadi.view.CursorLag;
import org.zalando.nakadi.view.ShiftedCursor;

import javax.validation.Valid;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController
public class CursorOperationsController {

    private final CursorConverter cursorConverter;
    private final CursorOperationsService cursorOperationsService;
    private final EventTypeRepository eventTypeRepository;
    private final AuthorizationValidator authorizationValidator;

    @Autowired
    public CursorOperationsController(final CursorOperationsService cursorOperationsService,
                                      final CursorConverter cursorConverter,
                                      final EventTypeRepository eventTypeRepository,
                                      final AuthorizationValidator authorizationValidator) {
        this.cursorOperationsService = cursorOperationsService;
        this.cursorConverter = cursorConverter;
        this.eventTypeRepository = eventTypeRepository;
        this.authorizationValidator = authorizationValidator;
    }

    @RequestMapping(path = "/event-types/{eventTypeName}/cursor-distances", method = RequestMethod.POST)
    public ResponseEntity<?> getDistance(@PathVariable("eventTypeName") final String eventTypeName,
                                         @Valid @RequestBody final ValidListWrapper<CursorDistance> queries)
            throws InternalNakadiException, NoSuchEventTypeException {

        final EventType eventType = eventTypeRepository.findByName(eventTypeName);
        authorizationValidator.authorizeEventTypeView(eventType);
        authorizationValidator.authorizeStreamRead(eventType);

        queries.getList().forEach(query -> {
            try {
                final NakadiCursor initialCursor = cursorConverter
                        .convert(eventTypeName, query.getInitialCursor());
                final NakadiCursor finalCursor = cursorConverter
                        .convert(eventTypeName, query.getFinalCursor());
                final Long distance = cursorOperationsService.calculateDistance(initialCursor, finalCursor);
                query.setDistance(distance);
            } catch (InternalNakadiException | ServiceTemporarilyUnavailableException e) {
                throw new NakadiBaseException("problem calculating cursors distance", e);
            } catch (final NoSuchEventTypeException e) {
                throw new NotFoundException("event type not found", e);
            } catch (final InvalidCursorException e) {
                throw new CursorConversionException("problem converting cursors", e);
            }
        });

        return ResponseEntity.status(HttpStatus.OK).body(queries.getList());
    }

    @RequestMapping(path = "/event-types/{eventTypeName}/shifted-cursors", method = RequestMethod.POST)
    public ResponseEntity<?> moveCursors(@PathVariable("eventTypeName") final String eventTypeName,
                                         @Valid @RequestBody final ValidListWrapper<ShiftedCursor> cursors)
            throws InternalNakadiException, NoSuchEventTypeException {

        final EventType eventType = eventTypeRepository.findByName(eventTypeName);
        authorizationValidator.authorizeEventTypeView(eventType);
        authorizationValidator.authorizeStreamRead(eventType);

        final List<ShiftedNakadiCursor> domainCursor = cursors.getList().stream()
                .map(this.toShiftedNakadiCursor(eventTypeName))
                .collect(Collectors.toList());

        final List<NakadiCursor> domainResultCursors = cursorOperationsService.unshiftCursors(domainCursor);

        final List<Cursor> viewResult = domainResultCursors.stream().map(cursorConverter::convert)
                .collect(Collectors.toList());

        return ResponseEntity.status(HttpStatus.OK).body(viewResult);
    }

    @RequestMapping(path = "/event-types/{eventTypeName}/cursors-lag", method = RequestMethod.POST)
    public List<CursorLag> cursorsLag(@PathVariable("eventTypeName") final String eventTypeName,
                                      @Valid @RequestBody final ValidListWrapper<Cursor> cursors)
            throws InternalNakadiException, NoSuchEventTypeException {

        final EventType eventType = eventTypeRepository.findByName(eventTypeName);
        authorizationValidator.authorizeEventTypeView(eventType);
        authorizationValidator.authorizeStreamRead(eventType);

        final List<NakadiCursor> domainCursor = cursors.getList().stream()
                .map(toNakadiCursor(eventTypeName))
                .collect(Collectors.toList());

        final List<NakadiCursorLag> lagResult = cursorOperationsService
                .cursorsLag(eventTypeName, domainCursor);

        return lagResult.stream().map(this::toCursorLag)
                .collect(Collectors.toList());
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
            } catch (final InternalNakadiException | InvalidCursorException e) {
                throw new CursorConversionException("problem converting cursors", e);
            }
        };
    }

    private Function<ShiftedCursor, ShiftedNakadiCursor> toShiftedNakadiCursor(final String eventTypeName) {
        return (cursor) -> {
            try {
                final NakadiCursor nakadiCursor = cursorConverter.convert(eventTypeName, cursor);
                return new ShiftedNakadiCursor(nakadiCursor.getTimeline(), nakadiCursor.getPartition(),
                        nakadiCursor.getOffset(), cursor.getShift());
            } catch (final InternalNakadiException | InvalidCursorException e) {
                throw new CursorConversionException("problem converting cursors", e);
            }
        };
    }
}
