package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.domain.Timeline;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@DB
@Repository
public class TimelineDbRepository extends AbstractDbRepository {

    public static final String BASE_TIMELINE_QUERY = "SELECT " +
            "   tl_id, et_name, tl_order, t.st_id, tl_topic, " +
            "   tl_created_at, tl_switched_at, tl_cleanup_at, tl_latest_position, " +
            "   st_type, st_configuration " +
            " FROM " +
            "   zn_data.timeline t " +
            "   JOIN zn_data.storage st USING (st_id) ";

    @Autowired
    public TimelineDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper jsonMapper) {
        super(jdbcTemplate, jsonMapper);
    }

    public List<Timeline> listTimelines() {
        return jdbcTemplate.query(BASE_TIMELINE_QUERY, timelineRowMapper);
    }

    public List<Timeline> listTimelines(final String eventType) {
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

    public Timeline createTimeline(final Timeline timeline) {
        if (null == timeline.getId()) {
            throw new IllegalArgumentException("Timeline id can not be null on create (" + timeline + ")");
        }
        try {
            jdbcTemplate.update(
                    "INSERT INTO zn_data.timeline(" +
                            " tl_id, et_name, tl_order, st_id, tl_topic, " +
                            " tl_created_at, tl_switched_at, tl_cleanup_at, tl_latest_position) " +
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)",
                    timeline.getId(),
                    timeline.getEventType(),
                    timeline.getOrder(),
                    timeline.getStorage().getId(),
                    timeline.getTopic(),
                    timeline.getCreatedAt(),
                    timeline.getSwitchedAt(),
                    timeline.getCleanupAt(),
                    timeline.getLatestPosition() == null ? null :
                            jsonMapper.writer().writeValueAsString(timeline.getLatestPosition()));
            return timeline;
        } catch (final JsonProcessingException ex) {
            throw new IllegalArgumentException("Can not serialize latest position to json", ex);
        }
    }

    public void updateTimelime(final Timeline timeline) {
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
                            " tl_latest_position=?::jsonb " +
                            " WHERE tl_id=?",
                    timeline.getEventType(),
                    timeline.getOrder(),
                    timeline.getStorage().getId(),
                    timeline.getTopic(),
                    timeline.getCreatedAt(),
                    timeline.getSwitchedAt(),
                    timeline.getCleanupAt(),
                    timeline.getLatestPosition() == null ? null :
                            jsonMapper.writer().writeValueAsString(timeline.getLatestPosition()),
                    timeline.getId());
        } catch (final JsonProcessingException ex) {
            throw new IllegalArgumentException("Can not serialize latest position to json", ex);
        }
    }

    public void deleteTimeline(final UUID id) {
        jdbcTemplate.update("DELETE FROM zn_data.timeline WHERE tl_id=?", id);
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
                            rs.getString("st_configuration")),
                    rs.getString("tl_topic"),
                    rs.getTimestamp("tl_created_at")
            );
            result.setId((UUID) rs.getObject("tl_id"));
            result.setCleanupAt(rs.getTimestamp("tl_cleanup_at"));
            result.setSwitchedAt(rs.getTimestamp("tl_switched_at"));
            result.setLatestPosition(
                    result.getStorage().restorePosition(jsonMapper, rs.getString("tl_latest_position")));
            return result;
        } catch (IOException e) {
            throw new SQLException("Failed to restore timeline with id " + timelineId, e);
        }
    };

}
