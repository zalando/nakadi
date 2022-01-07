package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.DuplicatedTimelineException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@DB
@Repository
public class TimelineDbRepository extends AbstractDbRepository {

    public static final String BASE_TIMELINE_QUERY = "SELECT " +
            "   tl_id, et_name, tl_order, t.st_id, tl_topic, tl_created_at, tl_switched_at, tl_cleanup_at, " +
            "   tl_latest_position, tl_deleted, st_type, st_configuration, st_default " +
            " FROM " +
            "   zn_data.timeline t " +
            "   JOIN zn_data.storage st USING (st_id) ";

    @Autowired
    public TimelineDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper jsonMapper) {
        super(jdbcTemplate, jsonMapper);
    }

    public List<Timeline> listTimelinesOrdered() {
        return jdbcTemplate.query(BASE_TIMELINE_QUERY + " order by t.et_name, t.tl_order", timelineRowMapper);
    }

    public List<Timeline> listTimelinesOrdered(final String eventType) {
        return jdbcTemplate.query(
                BASE_TIMELINE_QUERY + " WHERE t.et_name=? order by t.tl_order",
                timelineRowMapper,
                eventType);
    }

    public Optional<Timeline> getTimeline(final UUID id) {
        final List<Timeline> timelines = jdbcTemplate.query(
                BASE_TIMELINE_QUERY + " WHERE t.tl_id=?", timelineRowMapper, id);
        return Optional.ofNullable(timelines.isEmpty() ? null : timelines.get(0));
    }

    public Timeline createTimeline(final Timeline timeline)
            throws InconsistentStateException, RepositoryProblemException, DuplicatedTimelineException {
        if (null == timeline.getId()) {
            throw new IllegalArgumentException("Timeline id can not be null on create (" + timeline + ")");
        }
        try {
            jdbcTemplate.update(
                    "INSERT INTO zn_data.timeline(" +
                            " tl_id, et_name, tl_order, st_id, tl_topic, tl_created_at, tl_switched_at, " +
                            " tl_cleanup_at, tl_latest_position, tl_deleted) " +
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)",
                    timeline.getId(),
                    timeline.getEventType(),
                    timeline.getOrder(),
                    timeline.getStorage().getId(),
                    timeline.getTopic(),
                    timeline.getCreatedAt(),
                    timeline.getSwitchedAt(),
                    timeline.getCleanedUpAt(),
                    timeline.getLatestPosition() == null ? null :
                            jsonMapper.writer().writeValueAsString(timeline.getLatestPosition()),
                    timeline.isDeleted());
            return timeline;
        } catch (final JsonProcessingException e) {
            throw new InconsistentStateException("Can not serialize latest position to json", e);
        } catch (final DuplicateKeyException e) {
            throw new DuplicatedTimelineException("Timeline for this event-type with this order already exists", e);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Repository problem occurred when creating timeline", e);
        }
    }

    public void updateTimelime(final Timeline timeline) throws InconsistentStateException, RepositoryProblemException {
        if (null == timeline.getId()) {
            throw new IllegalArgumentException("Timeline id can not be null on update (" + timeline + ")");
        }

        try {
            jdbcTemplate.update(
                    "UPDATE zn_data.timeline SET " +
                            " et_name=?, " +
                            " tl_order=?, " +
                            " st_id=?, " +
                            " tl_topic=?, " +
                            " tl_created_at=?, " +
                            " tl_switched_at=?, " +
                            " tl_cleanup_at=?, " +
                            " tl_latest_position=?::jsonb, " +
                            " tl_deleted=? " +
                            " WHERE tl_id=?",
                    timeline.getEventType(),
                    timeline.getOrder(),
                    timeline.getStorage().getId(),
                    timeline.getTopic(),
                    timeline.getCreatedAt(),
                    timeline.getSwitchedAt(),
                    timeline.getCleanedUpAt(),
                    timeline.getLatestPosition() == null ? null :
                            jsonMapper.writer().writeValueAsString(timeline.getLatestPosition()),
                    timeline.isDeleted(),
                    timeline.getId());
        } catch (final JsonProcessingException ex) {
            throw new InconsistentStateException("Can not serialize timeline latest position to json", ex);
        } catch (final DataAccessException ex) {
            throw new RepositoryProblemException("Repository problem occurred when updating timeline", ex);
        }
    }

    public void deleteTimeline(final UUID id) {
        jdbcTemplate.update("DELETE FROM zn_data.timeline WHERE tl_id=?", id);
    }

    public List<Timeline> getExpiredTimelines() throws RepositoryProblemException {
        try {
            return jdbcTemplate.query(
                    BASE_TIMELINE_QUERY +
                            " WHERE tl_deleted = FALSE AND tl_cleanup_at IS NOT NULL AND tl_cleanup_at < now()" +
                            " ORDER BY t.et_name, t.tl_order",
                    timelineRowMapper);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("DB error occurred when fetching expired timelines", e);
        }
    }

    private final RowMapper<Timeline> timelineRowMapper = (rs, rowNum) -> {
        final UUID timelineId = (UUID) rs.getObject("tl_id");
        try {
            final Timeline result = new Timeline(
                    rs.getString("et_name"),
                    rs.getInt("tl_order"),
                    StorageDbRepository.buildStorage(
                            jsonMapper,
                            rs.getString("st_id"),
                            rs.getString("st_type"),
                            rs.getString("st_configuration"),
                            rs.getBoolean("st_default")),
                    rs.getString("tl_topic"),
                    rs.getTimestamp("tl_created_at")
            );
            result.setId((UUID) rs.getObject("tl_id"));
            result.setCleanedUpAt(rs.getTimestamp("tl_cleanup_at"));
            result.setSwitchedAt(rs.getTimestamp("tl_switched_at"));
            result.setDeleted(rs.getBoolean("tl_deleted"));
            result.setLatestPosition(
                    result.getStorage().restorePosition(jsonMapper, rs.getString("tl_latest_position")));
            return result;
        } catch (IOException e) {
            throw new SQLException("Failed to restore timeline with id " + timelineId, e);
        }
    };

}
