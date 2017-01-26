package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;

@Service
public class StorageService {

    private final ObjectMapper objectMapper;
    private final StorageDbRepository storageDbRepository;
    private final TimelineService timelineService;

    @Autowired
    public StorageService(final ObjectMapper objectMapper,
                          final StorageDbRepository storageDbRepository,
                          final TimelineService timelineService) {
        this.objectMapper = objectMapper;
        this.storageDbRepository = storageDbRepository;
        this.timelineService = timelineService;
    }

    public List<Storage> listStorages() {
        return storageDbRepository.listStorages();
    }

    public Result<Storage> getStorage(final String id) {
        final Optional<Storage> storage = storageDbRepository.getStorage(id);
        if (storage.isPresent()) {
            return Result.ok(storage.get());
        }
        else {
            return Result.problem(Problem.valueOf(Response.Status.NOT_FOUND, "No storage with id " + id));
        }
    }

    public Result<Storage> createStorage(final JSONObject storageDetails) {
        final String type;
        final JSONObject configuration;

        try {
            type = storageDetails.getString("storage_type");
            configuration = storageDetails.getJSONObject("configuration");
        } catch (JSONException e) {
            return Result.problem(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()));
        }

        final Storage storage = new Storage();
        storage.setId(UUID.randomUUID().toString());
        storage.setType(Storage.Type.valueOf(type.toUpperCase()));
        try {
            storage.parseConfiguration(objectMapper, configuration.toString());
        } catch (final IOException e) {
            return Result.problem(Problem.valueOf(UNPROCESSABLE_ENTITY, e.getMessage()));
        }

        storageDbRepository.createStorage(storage);

        return Result.ok(storage);
    }

    public Result<Void> deleteStorage(final String id) {
        if (!storageDbRepository.getStorage(id).isPresent()) {
            return Result.notFound("No storage with ID " + id);
        }
        if (isInUse(id)) {
            return Result.forbidden("Storage " + id + " is in use");
        }
        try {
            storageDbRepository.deleteStorage(id);
        } catch (DataAccessException e) {
            return Result.problem(Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage()));
        }
        return Result.ok();
    }

    private boolean isInUse(final String id) {
        final List<Timeline> timelines = timelineService.listTimelines();
        return timelines.stream().anyMatch(tl -> tl.getStorage().getId().equals(id));
    }
}
