package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.NoStorageException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.problem.Problem;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;

@Service
public class StorageService {

    private static final Logger LOG = LoggerFactory.getLogger(StorageService.class);

    private final ObjectMapper objectMapper;
    private final StorageDbRepository storageDbRepository;

    @Autowired
    public StorageService(final ObjectMapper objectMapper,
                          final StorageDbRepository storageDbRepository) {
        this.objectMapper = objectMapper;
        this.storageDbRepository = storageDbRepository;
    }

    public Result<List<Storage>> listStorages() {
        final List<Storage> storages;
        try {
            storages = storageDbRepository.listStorages();
        } catch (RepositoryProblemException e) {
            LOG.error("DB error occurred when listing storages", e);
            return Result.problem(Problem.valueOf(INTERNAL_SERVER_ERROR, e.getMessage()));
        }
        return Result.ok(storages);
    }

    public Result<Storage> getStorage(final String id) {
        final Optional<Storage> storage;
        try {
            storage = storageDbRepository.getStorage(id);
        } catch (RepositoryProblemException e) {
            LOG.error("DB error occurred when fetching storage", e);
            return Result.problem(Problem.valueOf(INTERNAL_SERVER_ERROR, e.getMessage()));
        }
        if (storage.isPresent()) {
            return Result.ok(storage.get());
        } else {
            return Result.problem(Problem.valueOf(NOT_FOUND, "No storage with id " + id));
        }
    }

    public Result<Void> createStorage(final JSONObject json) {
        final String type;
        final String id;
        final JSONObject configuration;

        try {
            id = json.getString("id");
            type = json.getString("storage_type");
            switch (type) {
                case "kafka":
                    configuration = json.getJSONObject("kafka_configuration");
                    break;
                default:
                    return Result.problem(Problem.valueOf(UNPROCESSABLE_ENTITY,
                            "Type '" + type + "' is not a valid storage type"));
            }
        } catch (JSONException e) {
            return Result.problem(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()));
        }

        final Storage storage = new Storage();
        storage.setId(id);
        storage.setType(Storage.Type.valueOf(type.toUpperCase()));
        try {
            storage.parseConfiguration(objectMapper, configuration.toString());
        } catch (final IOException e) {
            return Result.problem(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()));
        }

        try {
            storageDbRepository.createStorage(storage);
        } catch (final RepositoryProblemException e) {
            LOG.error("DB error occurred when creating storage", e);
            return Result.problem(Problem.valueOf(INTERNAL_SERVER_ERROR, e.getMessage()));
        }
        catch (final DuplicatedStorageException e) {
            return Result.problem(Problem.valueOf(CONFLICT, e.getMessage()));
        }
        return Result.ok();
    }

    public Result<Void> deleteStorage(final String id) {
        try {
            storageDbRepository.deleteStorage(id);
        } catch (final NoStorageException e) {
            return Result.notFound("No storage with ID " + id);
        } catch (final StorageIsUsedException e) {
            return Result.forbidden("Storage " + id + " is in use");
        } catch (final RepositoryProblemException e) {
            LOG.error("DB error occurred when deleting storage", e);
            return Result.problem(Problem.valueOf(INTERNAL_SERVER_ERROR, e.getMessage()));
        } catch (final TransactionException e) {
            LOG.error("Error with transaction handling when deleting storage", e);
            return Result.problem(Problem.valueOf(INTERNAL_SERVER_ERROR,
                    "Transaction error occurred when deleting storage"));
        }
        return Result.ok();
    }

}
