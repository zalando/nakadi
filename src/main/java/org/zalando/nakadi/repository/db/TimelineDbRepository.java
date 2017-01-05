package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.domain.Timeline;

@DB
@Repository
public class TimelineDbRepository extends AbstractDbRepository {

    public static final String BASE_TIMELINE_QUERY = "SELECT " +
            "   t_id, et_name, t_order, t.st_id, t_topic, " +
            "   t_created_at, t_switched_at, t_cleanup_at, t_latest_position, " +
            "   st_type, st_configuration " +
            " FROM " +
            "   zn_data.timeline t " +
            "   JOIN zn_data.storage st USING (st_id) ";

    @Autowired
    public TimelineDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper jsonMapper) {
        super(jdbcTemplate, jsonMapper);
    }

    public List<Timeline> listTimelines(final String eventType) {
        return jdbcTemplate.query(
                BASE_TIMELINE_QUERY + " WHERE t.et_name=? order by t.t_order",
                new Object[]{eventType},
                timelineRowMapper);
    }

    public Optional<Timeline> getTimeline(final UUID id) {
        final List<Timeline> timelines = jdbcTemplate.query(
                BASE_TIMELINE_QUERY + " WHERE t.t_id=?",
                new Object[]{id},
                timelineRowMapper);
        return Optional.ofNullable(timelines.isEmpty() ? null : timelines.get(0));
    }

    public Timeline createTimeline(final Timeline timeline) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO zn_data.timeline(" +
                            " t_id, et_name, t_order, st_id, t_topic, " +
                            " t_created_at, t_switched_at, t_cleanup_at, t_latest_position) " +
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
        try {
            jdbcTemplate.update(
                    "UPDATE zn_data.timeline SET " +
                            " et_name=?, " +
                            " t_order=?, " +
                            " st_id=?, " +
                            " t_topic=?, " +
                            " t_created_at=?, " +
                            " t_switched_at=?, " +
                            " t_cleanup_at=?, " +
                            " t_latest_position=?::jsonb " +
                            " WHERE t_id=?",
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
        jdbcTemplate.update(
                "DELETE FROM zn_data.timeline WHERE t_id=?",
                new Object[]{id});
    }

    private final RowMapper<Timeline> timelineRowMapper = (rs, rowNum) -> {
        final UUID timelineId = (UUID) rs.getObject("t_id");
        try {
            final Timeline result = new Timeline();
            result.setStorage(StorageDbRepository.buildStorage(
                    jsonMapper,
                    rs.getString("st_id"),
                    rs.getString("st_type"),
                    rs.getString("st_configuration")));
            result.setId((UUID) rs.getObject("t_id"));
            result.setEventType(rs.getString("et_name"));
            result.setOrder(rs.getInt("t_order"));
            result.setTopic(rs.getString("t_topic"));
            result.setCreatedAt(rs.getTimestamp("t_created_at"));
            result.setCleanupAt(rs.getTimestamp("t_cleanup_at"));
            result.setSwitchedAt(rs.getTimestamp("t_switched_at"));
            result.setLatestPosition(
                    result.getStorage().restorePosition(jsonMapper, rs.getString("t_latest_position")));
            return result;
        } catch (IOException e) {
            throw new SQLException("Failed to restore timeline with id " + timelineId, e);
        }
    };
}
