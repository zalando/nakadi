package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.InternalNakadiException;

@DB
@Repository
public class StorageDbRepository extends AbstractDbRepository {
    @Autowired
    public StorageDbRepository(final JdbcTemplate template, final ObjectMapper mapper) {
        super(template, mapper);
    }

    public List<Storage> listStorages() throws InternalNakadiException {
        List<Storage> storages;
        try {
            storages = jdbcTemplate.query("SELECT st_id, st_type, st_configuration FROM zn_data.storage ORDER BY st_id",
                    storageRowMapper);
        } catch (DataAccessException e) {
            throw new InternalNakadiException("Error occurred when fetching list of Storages", e);
        }

        return storages;
    }

    public Optional<Storage> getStorage(final String id) throws InternalNakadiException {
        final List<Storage> storages;
        try {
            storages = jdbcTemplate.query(
                    "SELECT st_id, st_type, st_configuration FROM zn_data.storage WHERE st_id=?",
                    storageRowMapper,
                    id);
        } catch (DataAccessException e) {
            throw new InternalNakadiException("Error occurred when fetching Storage " + id, e);
        }
        return Optional.ofNullable(storages.isEmpty() ? null : storages.get(0));
    }

    public Storage createStorage(final Storage storage) throws DataAccessException, InternalNakadiException {
        try {
            jdbcTemplate.update(
                    "INSERT INTO zn_data.storage (st_id, st_type, st_configuration) VALUES (?, ?, ?::jsonb)",
                    storage.getId(),
                    storage.getType().name(),
                    jsonMapper.writer().writeValueAsString(storage.getConfiguration(Object.class)));
            return storage;
        } catch (final JsonProcessingException ex) {
            throw new IllegalArgumentException("Storage configuration " + storage.getConfiguration(Object.class) +
                    " can't be mapped to json", ex);
        } catch (DataAccessException e) {
            throw new InternalNakadiException("Error occurred when creating Storage " + storage.getId(), e);
        }
    }

    public void deleteStorage(final String id) throws InternalNakadiException {
        try {
            jdbcTemplate.update("DELETE FROM zn_data.storage WHERE st_id=?", id);
        } catch (DataAccessException e) {
            throw new InternalNakadiException("Error occurred when deleting Storage " + id, e);
        }
    }

    public boolean isStorageUsed(final String id) throws InternalNakadiException {
        boolean used;
        try {
            used = !jdbcTemplate.queryForList("SELECT FROM zn_data.timeline WHERE st_id=?", id).isEmpty();
        } catch (DataAccessException e) {
            throw new InternalNakadiException("Error occurred when querying for Storage " + id, e);
        }
        return used;
    }

    static Storage buildStorage(final ObjectMapper mapper, final String id, final String type, final String config)
            throws SQLException {
        final Storage result = new Storage();
        result.setId(id);
        result.setType(Storage.Type.valueOf(type));
        try {
            result.parseConfiguration(mapper, config);
        } catch (final IOException ex) {
            throw new SQLException("Failed to restore storage with id " + result.getId(), ex);
        }
        return result;
    }

    private final RowMapper<Storage> storageRowMapper = (rs, rowNum) -> buildStorage(
            jsonMapper,
            rs.getString("st_id"),
            rs.getString("st_type"),
            rs.getString("st_configuration"));
}
