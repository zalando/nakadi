package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

@DB
@Repository
public class StorageDbRepository extends AbstractDbRepository {
    @Autowired
    public StorageDbRepository(final JdbcTemplate template, final ObjectMapper mapper) {
        super(template, mapper);
    }

    private static final String STORAGE_FIELDS = "st_id, st_type, st_configuration, st_default";

    public List<Storage> listStorages() throws RepositoryProblemException {
        final List<Storage> storages;
        try {
            storages = jdbcTemplate.query("SELECT " + STORAGE_FIELDS + " FROM zn_data.storage ORDER BY st_id",
                    storageRowMapper);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when fetching list of Storages", e);
        }

        return storages;
    }

    public Optional<Storage> getDefaultStorage() throws RepositoryProblemException {
        final List<Storage> storages = jdbcTemplate.query(
                "SELECT " + STORAGE_FIELDS + " FROM zn_data.storage WHERE st_default=? order by st_id limit 1",
                storageRowMapper,
                true);
        if (!storages.isEmpty()) {
            return Optional.of(storages.get(0));
        }
        return Optional.empty();
    }

    public Optional<Storage> getStorage(final String id) throws RepositoryProblemException {
        final List<Storage> storages;
        try {
            storages = jdbcTemplate.query(
                    "SELECT " + STORAGE_FIELDS + " FROM zn_data.storage WHERE st_id=?",
                    storageRowMapper,
                    id);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when fetching Storage " + id, e);
        }
        return Optional.ofNullable(storages.isEmpty() ? null : storages.get(0));
    }

    public Storage createStorage(final Storage storage) throws DuplicatedStorageException, RepositoryProblemException {
        try {
            jdbcTemplate.update(
                    "INSERT INTO zn_data.storage (st_id, st_type, st_configuration, st_default) " +
                            "VALUES (?, ?, ?::jsonb, ?)",
                    storage.getId(),
                    storage.getType().name(),
                    jsonMapper.writer().writeValueAsString(storage.getConfiguration(Object.class)),
                    storage.isDefault());
            return storage;
        } catch (final JsonProcessingException ex) {
            throw new IllegalArgumentException("Storage configuration " + storage.getConfiguration(Object.class) +
                    " can't be mapped to json", ex);
        } catch (final DuplicateKeyException e) {
            throw new DuplicatedStorageException("A storage with id '" + storage.getId() + "' already exists.", e);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when creating Storage " + storage.getId(), e);
        }
    }

    public void setDefaultStorage(final String storageId, final boolean isDefault) {
        try {
            jdbcTemplate.update("UPDATE zn_data.storage SET st_default=? WHERE st_id=?",
                    isDefault,
                    storageId);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when creating Storage " + storageId, e);
        }
    }

    public void deleteStorage(final String id)
            throws NoSuchStorageException, StorageIsUsedException, RepositoryProblemException {
        try {
            final int rowDeleted = jdbcTemplate.update("DELETE FROM zn_data.storage WHERE st_id=?", id);
            if (rowDeleted == 0) {
                throw new NoSuchStorageException("Tried to remove storage that doesn't exist, id: " + id);
            }
        } catch (final DataIntegrityViolationException e) {
            throw new StorageIsUsedException("Can't delete storage as it is still used, id: " + id, e);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when deleting Storage " + id, e);
        }
    }

    static Storage buildStorage(
            final ObjectMapper mapper,
            final String id,
            final String type,
            final String config,
            final boolean isDefault)
            throws SQLException {
        final Storage result = new Storage();
        result.setId(id);
        result.setType(Storage.Type.valueOf(type));
        result.setDefault(isDefault);
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
            rs.getString("st_configuration"),
            rs.getBoolean("st_default"));
}
