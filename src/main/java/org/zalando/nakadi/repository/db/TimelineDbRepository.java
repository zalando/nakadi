package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

@DB
@Repository
public class TimelineDbRepository extends AbstractDbRepository {
    @Autowired
    public TimelineDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper jsonMapper) {
        super(jdbcTemplate, jsonMapper);
    }

    public List<Storage> getStorages() {
        return jdbcTemplate.query("select st_id, st_type, st_configuration from zn_data.storage order by st_id",
                storageRowMapper);
    }

    public List<Timeline> getTimelines(
            final String eventType,
            @Nullable final Boolean active) {
        String sqlQuery = "select " +
                "   t_id, et_name, t_order, t.st_id, t_storage_configuration, " +
                "   t_created_at, t_switched_at, t_freed_at, t_latest_position, " +
                "   st_type, st_configuration " +
                " from " +
                "   zn_data.timeline t " +
                "   join zn_data.storage st using (st_id) " +
                " where " +
                "   t.et_name = ? ";
        if (null != active) {
            sqlQuery += active ? " and t_freed_at is null " : " and t_freed_at is not null";
        }
        sqlQuery += " order by t_order";
        return jdbcTemplate.query(sqlQuery, new Object[]{eventType}, timelineRowMapper);
    }

    private final RowMapper<Storage> storageRowMapper = (rs, rowNum) -> {
        final String id = rs.getString("st_id");
        try {
            final Storage.Type storageType = Storage.Type.valueOf(rs.getString("st_type"));
            final Storage result;
            switch (storageType) {
                case KAFKA:
                    result = jsonMapper.readValue(rs.getString("st_configuration"), Storage.KafkaStorage.class);
                    break;
                default:
                    throw new SQLException("Failed to read storage with id " + id + ", storage type " + storageType +
                            " is not supported");
            }
            result.setType(storageType);
            result.setId(id);
            return result;
        } catch (IOException e) {
            throw new SQLException("Failed to restore storage with id " + id, e);
        }
    };

    private final RowMapper<Timeline> timelineRowMapper = (rs, rowNum) -> {
        final Storage storage = storageRowMapper.mapRow(rs, rowNum);
        final Integer timelineId = rs.getInt("t_id");
        try {
            final Timeline result = new Timeline();
            result.setId(timelineId);
            result.setEventType(rs.getString("et_name"));
            result.setOrder(rs.getInt("t_order"));
            result.setStorage(storage);
            result.setEventTypeConfiguration(
                    storage.restoreEventTypeConfiguration(jsonMapper, rs.getString("t_storage_configuration")));
            result.setCreatedAt(rs.getTimestamp("t_created_at"));
            result.setCleanupAt(rs.getTimestamp("t_cleanup_at"));
            result.setSwitchedAt(rs.getTimestamp("t_switched_at"));
            result.setFreedAt(rs.getTimestamp("t_freed_at"));
            result.setStoragePosition(storage.restorePosition(jsonMapper, rs.getString("t_latest_position")));
            return result;
        } catch (IOException e) {
            throw new SQLException("Failed to restore timeline with id " + timelineId, e);
        }
    };
}
