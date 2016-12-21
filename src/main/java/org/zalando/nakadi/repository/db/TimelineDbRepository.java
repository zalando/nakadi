package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@DB
@Repository
public class TimelineDbRepository extends AbstractDbRepository {

    public static final String BASE_TIMELINE_QUERY = "select " +
            "   t_id, et_name, t_order, t.st_id, t_storage_configuration, " +
            "   t_created_at, t_switched_at, t_freed_at, t_latest_position, " +
            "   st_type, st_configuration " +
            " from " +
            "   zn_data.timeline t " +
            "   join zn_data.storage st using (st_id) ";

    private final Storage.KafkaStorage defaultStorage;

    @Autowired
    public TimelineDbRepository(
            final JdbcTemplate jdbcTemplate,
            final ObjectMapper jsonMapper,
            final Environment environment) { // Here is a little piece of 'code' with env.
        super(jdbcTemplate, jsonMapper);
        defaultStorage = (Storage.KafkaStorage) getStorage("default").orElseGet(
                () -> createDefaultStorage(
                        environment.getProperty("nakadi.zookeeper.brokers"),
                        environment.getProperty("nakadi.zookeeper.kafkaNamespace"),
                        environment.getProperty("nakadi.zookeeper.exhibitor.brokers"),
                        Integer.parseInt(environment.getProperty("nakadi.zookeeper.exhibitor.port", "0"))));
    }

    private Storage.KafkaStorage createDefaultStorage(final String zkAddress, final String zkPath, final String exhibitorAddress, final Integer exhibitorPort) {
        try {
            final Storage.KafkaStorage result = new Storage.KafkaStorage(zkAddress, zkPath, exhibitorAddress, exhibitorPort);
            create(result);
            return result;
        } catch (final InternalNakadiException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Storage.KafkaStorage getDefaultStorage() {
        return defaultStorage;
    }

    public List<Storage> getStorages() {
        return jdbcTemplate.query("select st_id, st_type, st_configuration from zn_data.storage order by st_id",
                storageRowMapper);
    }

    public Optional<Storage> getStorage(final String storageId) {
        final List<Storage> result = jdbcTemplate.query(
                "select st_id, st_type, st_configuration from zn_data.storage where st_id=?",
                new Object[]{storageId},
                storageRowMapper);
        return Optional.ofNullable(result.isEmpty() ? null : result.get(0));
    }

    @Nullable
    public Timeline loadActiveTimeline(
            final String eventType) {
        final List<Timeline> result = jdbcTemplate.query(
                BASE_TIMELINE_QUERY + " where t.et_name=? and t_freed_at is null order by t_oder desc limit 1",
                new Object[]{eventType},
                timelineRowMapper);
        return result.isEmpty() ? null : result.get(0);
    }

    @Nullable
    public Timeline getTimeline(final Integer id) {
        final List<Timeline> result = jdbcTemplate.query(BASE_TIMELINE_QUERY + " where t.t_id=?",
                new Object[]{id},
                timelineRowMapper);
        return result.isEmpty() ? null : result.get(0);
    }

    public List<Timeline> listTimelines(
            @Nullable final String eventType,
            @Nullable final String storageId,
            @Nullable final Boolean active,
            @Nullable final String timelineId) {
        final List<String> filters = new ArrayList<>();
        final List<Object> arguments = new ArrayList<>();
        if (null != eventType) {
            arguments.add(eventType);
            filters.add("t.et_name=?");
        }
        if (null != active) {
            filters.add(active ? "t_freed_at is null " : "t_freed_at is not null");
        }
        if (null != timelineId) {
            arguments.add(timelineId);
            filters.add("t.st_id=?");
        }
        if (null != storageId) {
            arguments.add(storageId);
            filters.add("t.st_id=?");
        }
        final String filterQuery = filters.isEmpty() ? "" :
                (" WHERE " + filters.stream().collect(Collectors.joining(" and ")));
        return jdbcTemplate.query(
                BASE_TIMELINE_QUERY + filterQuery + "order by t_order desc",
                arguments.toArray(),
                timelineRowMapper);
    }

    public void create(final Storage storage) throws InternalNakadiException {
        try {
            jdbcTemplate.update(
                    "INSERT into zn_data.storage(st_id, st_type, st_configuration) values (?, ?, ?::jsonb)",
                    storage.getId(),
                    storage.getType(),
                    jsonMapper.writer().writeValueAsString(storage.createSpecificConfiguration())
            );
        } catch (final JsonProcessingException ex) {
            throw new InternalNakadiException("Serialization problem during persistence of storage", ex);
        }
    }

    public void update(final Storage storage) throws InternalNakadiException {
        try {
            final int affectedRows = jdbcTemplate.update(
                    "UPDATE zn_data.storage set st_type=?, st_configuration=? where st_id=?",
                    storage.getType(),
                    jsonMapper.writer().writeValueAsString(storage.createSpecificConfiguration()),
                    storage.getId());
            if (affectedRows != 1) {
                throw new InternalNakadiException(
                        "Updated rows: " + affectedRows + ", expected: 1 while updating storage " + storage.getId());
            }
        } catch (final JsonProcessingException ex) {
            throw new InternalNakadiException("Serialization problem during persistence of storage", ex);
        }
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
            result.setStorageConfiguration(
                    storage.restoreEventTypeConfiguration(jsonMapper, rs.getString("t_storage_configuration")));
            result.setCreatedAt(rs.getTimestamp("t_created_at"));
            result.setCleanupAt(rs.getTimestamp("t_cleanup_at"));
            result.setSwitchedAt(rs.getTimestamp("t_switched_at"));
            result.setFreedAt(rs.getTimestamp("t_freed_at"));
            result.setLastPosition(storage.restorePosition(timelineId, rs.getString("t_latest_position")));
            return result;
        } catch (IOException e) {
            throw new SQLException("Failed to restore timeline with id " + timelineId, e);
        }
    };

    public void delete(final Storage storage) {
        jdbcTemplate.update("delete from storage where st_id=?", new Object[]{storage.getId()});
    }

    public void update(final Timeline timeline) {
        final String sqlUpdate = "UPDATE zn_data.timeline SET " +
                "et_name=?," +
                "t_order=?, " +
                "st_id=?, " +
                "t_storage_configuration=?, " +
                "t_created_at=?, " +
                "t_cleanup_at=?, " +
                "t_switched_at=?, " +
                "t_freed_at=?, " +
                "t_latest_position=?" +
                " WHERE t_id=?";
        try {
            final int updated = jdbcTemplate.update(sqlUpdate,
                    timeline.getEventType(),
                    timeline.getOrder(),
                    timeline.getStorage().getId(),
                    jsonMapper.writeValueAsString(timeline.getStorageConfiguration()),
                    timeline.getCreatedAt(),
                    timeline.getCleanupAt(),
                    timeline.getSwitchedAt(),
                    timeline.getFreedAt(),
                    timeline.getStorage().storePosition(timeline.getLastPosition()),
                    timeline.getId());
            if (updated < 1) {
                throw new RuntimeException("No timeline update performed for id " + timeline.getId());
            }
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to update timeline configuration", ex);
        }
    }

    public Timeline create(final Timeline timeline) {
        final String sqlCreate = "INSERT into zn_data.timeline(" +
                "et_name, " +
                "t_order, " +
                "st_id, " +
                "t_storage_configuration, " +
                "t_created_at, " +
                "t_cleanup_at, " +
                "t_switched_at, " +
                "t_freed_at, " +
                "t_latest_position)" +
                " values (?,?,?,?::jsonb,?,?,?,?,?)";
        final KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            try {
                final PreparedStatement ps = con.prepareStatement(sqlCreate, new String[]{"t_id"});
                ps.setString(1, timeline.getEventType());
                ps.setInt(2, timeline.getOrder());
                ps.setString(3, timeline.getStorage().getId());
                ps.setString(4, jsonMapper.writeValueAsString(timeline.getStorageConfiguration()));
                ps.setTimestamp(5, dateToTimestamp(timeline.getCreatedAt()));
                ps.setTimestamp(6, dateToTimestamp(timeline.getCleanupAt()));
                ps.setTimestamp(7, dateToTimestamp(timeline.getSwitchedAt()));
                ps.setTimestamp(8, dateToTimestamp(timeline.getFreedAt()));
                ps.setString(9, timeline.getStorage().storePosition(timeline.getLastPosition()));
                return ps;
            } catch (final IOException ex) {
                throw new RuntimeException("Failed to serialize timeline", ex);
            }
        }, keyHolder);
        return getTimeline(keyHolder.getKey().intValue());
    }

    private static Timestamp dateToTimestamp(final Date date) {
        return null == date ? null : new Timestamp(date.getTime());
    }

    public void delete(final Timeline timeline) {
        final int updated = jdbcTemplate.update("delete from zn_data.timeline where t_id=?", timeline.getId());
        if (updated != 1) {
            throw new IllegalArgumentException("Record to delete not found");
        }
    }
}
