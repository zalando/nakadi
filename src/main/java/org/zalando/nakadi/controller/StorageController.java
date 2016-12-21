package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.service.timeline.TimelineService;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/storages")
public class StorageController {

    public static class StorageView {
        private String id;
        @NotNull
        private String type;
        @Nullable
        private String kafkaZookeeper;
        @Nullable
        private String kafkaZookeeperPath;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getType() {
            return type;
        }

        public void setType(final String type) {
            this.type = type;
        }

        @Nullable
        public String getKafkaZookeeper() {
            return kafkaZookeeper;
        }

        public void setKafkaZookeeper(@Nullable final String kafkaZookeeper) {
            this.kafkaZookeeper = kafkaZookeeper;
        }

        @Nullable
        public String getKafkaZookeeperPath() {
            return kafkaZookeeperPath;
        }

        public void setKafkaZookeeperPath(@Nullable final String kafkaZookeeperPath) {
            this.kafkaZookeeperPath = kafkaZookeeperPath;
        }

        public static StorageView build(final Storage storage) {
            if (null == storage) {
                return null;
            }
            final StorageView result = new StorageView();
            result.setId(storage.getId());
            result.setType(storage.getType().name());
            switch (storage.getType()) {
                case KAFKA:
                    result.setKafkaZookeeper(((Storage.KafkaStorage) storage).getZkAddress());
                    result.setKafkaZookeeperPath(((Storage.KafkaStorage) storage).getZkPath());
                    break;
            }
            return result;
        }
    }

    private final TimelineService timelineService;

    @Autowired
    public StorageController(final TimelineService timelineService) {
        this.timelineService = timelineService;
    }

    private Storage mapToBusinessEntity(final StorageView storage) {
        final Storage.Type type = Storage.Type.valueOf(storage.getType());
        switch (type) {
            case KAFKA:
                return new Storage.KafkaStorage(storage.getKafkaZookeeper(), storage.getKafkaZookeeperPath());
            default:
                throw new IllegalArgumentException("storage type " + type + " is not supported");
        }
    }

    @RequestMapping(method = RequestMethod.POST)
    public void createStorage(@RequestBody @Valid final StorageView storage) throws NakadiException {
        final Storage toSave = mapToBusinessEntity(storage);
        toSave.setId(storage.getId());
        timelineService.createOrUpdateStorage(toSave);
    }

    @RequestMapping(method = RequestMethod.GET)
    public List<StorageView> listStorages() {
        return timelineService.listStorages().stream().map(StorageView::build).collect(Collectors.toList());
    }

    @RequestMapping(path = "/{storage_id:.+}", method = RequestMethod.GET)
    public StorageView getStorage(@PathVariable("storage_id") final String storageId) {
        return timelineService.getStorage(storageId).map(StorageView::build)
                .orElseThrow(() -> new RuntimeException("Not found"));
    }

    @RequestMapping(path = "/{storage_id:.+}", method = RequestMethod.DELETE)
    public void deleteStorage(@PathVariable("storage_id") final String storageId) {
        timelineService.deleteStorage(storageId);
    }

    @RequestMapping(path = "/{storage_id:.+}", method = RequestMethod.PUT)
    public void updateStorage(
            final String storageId,
            @RequestBody @Valid final StorageView storageView) throws NakadiException {
        final Storage toUpdate = mapToBusinessEntity(storageView);
        toUpdate.setId(storageId);
        timelineService.createOrUpdateStorage(toUpdate);
    }
}
