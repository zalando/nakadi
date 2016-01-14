package de.zalando.aruha.nakadi.repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.domain.Cursor;

public interface EventConsumer {

    void setCursors(List<Cursor> cursors);

    Optional<ConsumedEvent> readEvent() throws NakadiException;

    Map<String, String> fetchNextOffsets();

}
