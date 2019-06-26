package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.zalando.nakadi.domain.storage.DefaultStorage;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.exceptions.runtime.UnknownStorageTypeException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableEntityException;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@Service
public class StorageService {

    public static final String ZK_TIMELINES_DEFAULT_STORAGE = "/nakadi/timelines/default_storage";
    private static final Logger LOG = LoggerFactory.getLogger(StorageService.class);
    private final ObjectMapper objectMapper;
    private final StorageDbRepository storageDbRepository;
    private final DefaultStorage defaultStorage;
    private final CuratorFramework curator;
    private final FeatureToggleService featureToggleService;
    private final NakadiAuditLogPublisher auditLogPublisher;

    @Autowired
    public StorageService(final ObjectMapper objectMapper,
                          final StorageDbRepository storageDbRepository,
                          @Qualifier("default_storage") final DefaultStorage defaultStorage,
                          final ZooKeeperHolder zooKeeperHolder,
                          final FeatureToggleService featureToggleService,
                          final NakadiAuditLogPublisher auditLogPublisher) {
        this.objectMapper = objectMapper;
        this.storageDbRepository = storageDbRepository;
        this.defaultStorage = defaultStorage;
        this.curator = zooKeeperHolder.get();
        this.featureToggleService = featureToggleService;
        this.auditLogPublisher = auditLogPublisher;
    }

    @PostConstruct
    private void watchDefaultStorage() {
        try {
            curator.getData().usingWatcher((CuratorWatcher) event -> {
                final byte[] defaultStorageId = curator.getData().forPath(ZK_TIMELINES_DEFAULT_STORAGE);
                if (defaultStorageId != null) {
                    final Storage storage = getStorage(new String(defaultStorageId));
                    defaultStorage.setStorage(storage);
                }
                watchDefaultStorage();
            }).forPath(ZK_TIMELINES_DEFAULT_STORAGE);
        } catch (final Exception e) {
            LOG.warn("Error while creating watcher for default storage updates {}", e.getMessage(), e);
        }
    }

    public List<Storage> listStorages() throws InternalNakadiException {
        final List<Storage> storages;
        try {
            storages = storageDbRepository.listStorages();
        } catch (RepositoryProblemException e) {
            LOG.error("DB error occurred when listing storages", e);
            throw new InternalNakadiException(e.getMessage());
        }
        return storages;
    }

    public Storage getStorage(final String id) throws NoSuchStorageException, InternalNakadiException {
        final Optional<Storage> storage;
        try {
            storage = storageDbRepository.getStorage(id);
        } catch (RepositoryProblemException e) {
            LOG.error("DB error occurred when fetching storage", e);
            throw new InternalNakadiException(e.getMessage());
        }
        if (storage.isPresent()) {
            return storage.get();
        } else {
            throw new NoSuchStorageException("No storage with id " + id);
        }
    }

    public void createStorage(final JSONObject json)
            throws DbWriteOperationsBlockedException, DuplicatedStorageException, InternalNakadiException,
            UnknownStorageTypeException {
        if (featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot create storage: write operations on DB " +
                    "are blocked by feature flag.");
        }
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
                    throw new UnknownStorageTypeException("Type '" + type + "' is not a valid storage type");
            }
        } catch (JSONException e) {
            throw new UnprocessableEntityException(e.getMessage());
        }

        final Storage storage = new Storage();
        storage.setId(id);
        storage.setType(Storage.Type.valueOf(type.toUpperCase()));
        try {
            storage.parseConfiguration(objectMapper, configuration.toString());
        } catch (final IOException e) {
            throw new UnprocessableEntityException(e.getMessage());
        }

        try {
            storageDbRepository.createStorage(storage);
        } catch (final RepositoryProblemException e) {
            LOG.error("DB error occurred when creating storage", e);
            throw new InternalNakadiException(e.getMessage());
        }

        auditLogPublisher.publish(
                Optional.empty(),
                Optional.of(storage),
                NakadiAuditLogPublisher.ResourceType.STORAGE,
                NakadiAuditLogPublisher.ActionType.CREATED,
                storage.getId());
    }

    public void deleteStorage(final String id)
            throws DbWriteOperationsBlockedException, NoSuchStorageException,
            StorageIsUsedException, InternalNakadiException {
        if (featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot delete storage: write operations on DB " +
                    "are blocked by feature flag.");
        }
        try {
            final Optional<Object> storageOrNone = storageDbRepository.getStorage(id)
                    .map(Function.identity());

            storageDbRepository.deleteStorage(id);

            auditLogPublisher.publish(
                    storageOrNone,
                    Optional.empty(),
                    NakadiAuditLogPublisher.ResourceType.STORAGE,
                    NakadiAuditLogPublisher.ActionType.DELETED,
                    id);
        } catch (final RepositoryProblemException e) {
            LOG.error("DB error occurred when deleting storage", e);
            throw new InternalNakadiException(e.getMessage());
        } catch (final TransactionException e) {
            LOG.error("Error with transaction handling when deleting storage", e);
            throw new InternalNakadiException("Transaction error occurred when deleting storage");
        }
    }

    public Storage setDefaultStorage(final String defaultStorageId)
            throws NoSuchStorageException, InternalNakadiException {
        final Storage storage = getStorage(defaultStorageId);
        try {
            curator.setData().forPath(ZK_TIMELINES_DEFAULT_STORAGE, defaultStorageId.getBytes(Charsets.UTF_8));
        } catch (final Exception e) {
            LOG.error("Error while setting default storage in zk {} ", e.getMessage(), e);
            throw new InternalNakadiException("Error while setting default storage in zk");
        }
        return storage;
    }
}
