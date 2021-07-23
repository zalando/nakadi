package org.zalando.nakadi.controller.util;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorLag;
import org.zalando.nakadi.exceptions.runtime.CursorConversionException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.CursorLag;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CursorUtil {

    public static Stream<CursorLag> toCursorLagStream(final List<Cursor> cursorList, final String eventTypeName,
                                                      final CursorConverter cursorConverter,
                                                      final CursorOperationsService cursorOperationsService){
        final List<NakadiCursor> domainCursor = cursorList.stream()
                .map(CursorUtil.toNakadiCursor(eventTypeName, cursorConverter))
                .collect(Collectors.toList());

        final List<NakadiCursorLag> lagResult = cursorOperationsService
                .cursorsLag(eventTypeName, domainCursor);

        return lagResult.stream().map(ncl -> CursorUtil.toCursorLag(ncl, cursorConverter));

    }


    private static CursorLag toCursorLag(final NakadiCursorLag nakadiCursorLag, final CursorConverter cursorConverter) {
        return new CursorLag(
                nakadiCursorLag.getPartition(),
                cursorConverter.convert(nakadiCursorLag.getFirstCursor()).getOffset(),
                cursorConverter.convert(nakadiCursorLag.getLastCursor()).getOffset(),
                nakadiCursorLag.getLag()
        );
    }

    private static Function<Cursor, NakadiCursor> toNakadiCursor(final String eventTypeName,
                                                                 final CursorConverter cursorConverter) {
        return cursor -> {
            try {
                return cursorConverter.convert(eventTypeName, cursor);
            } catch (final InternalNakadiException | InvalidCursorException e) {
                throw new CursorConversionException("problem converting cursors", e);
            }
        };
    }

}
