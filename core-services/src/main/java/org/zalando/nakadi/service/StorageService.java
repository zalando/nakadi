package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.storage.KafkaConfiguration;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.exceptions.runtime.UnknownStorageTypeException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableEntityException;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@Service("storageService")
public class StorageService {

    private static final Logger LOG = LoggerFactory.getLogger(StorageService.class);
    private final ObjectMapper objectMapper;
    private final StorageDbRepository storageDbRepository;
    private final FeatureToggleService featureToggleService;
    private final NakadiAuditLogPublisher auditLogPublisher;
    private final TransactionTemplate transactionTemplate;
    private final Environment environment;

    @Autowired
    public StorageService(final ObjectMapper objectMapper,
                          final StorageDbRepository storageDbRepository,
                          final FeatureToggleService featureToggleService,
                          final NakadiAuditLogPublisher auditLogPublisher,
                          final TransactionTemplate transactionTemplate,
                          final Environment environment) {
        this.objectMapper = objectMapper;
        this.storageDbRepository = storageDbRepository;
        this.featureToggleService = featureToggleService;
        this.auditLogPublisher = auditLogPublisher;
        this.transactionTemplate = transactionTemplate;
        this.environment = environment;
    }

    @PostConstruct
    public void initializeDefaultStorage() {
        if (!storageDbRepository.listStorages().isEmpty()) {
            return;
        }
        final String defaultStorageId = environment.getProperty("nakadi.timelines.storage.default");

        LOG.info("Creating storage `{}` from defaults", defaultStorageId);
        final Storage storage = new Storage();
        storage.setId(defaultStorageId);
        storage.setType(Storage.Type.KAFKA);
        storage.setConfiguration(new KafkaConfiguration(
                ZookeeperConnection.valueOf(environment.getProperty("nakadi.zookeeper.connection-string"))));
        storage.setDefault(true);
        storageDbRepository.createStorage(storage);
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
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot create storage: write operations on DB " +
                    "are blocked by feature flag.");
        }
        final String type;
        final String id;
        final JSONObject configuration;
        final boolean isDefault;

        try {
            id = json.getString("id");
            type = json.getString("storage_type");
            isDefault = json.has("default") ? json.getBoolean("default") : false;
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
        storage.setDefault(isDefault);
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
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
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
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot change default storage: write operations on DB " +
                    "are blocked by feature flag.");
        }

        return transactionTemplate.execute((status) -> {
            final List<Storage> allStorages = storageDbRepository.listStorages();
            final Optional<Storage> newDefaultStorageOpt = allStorages.stream()
                    .filter(s -> s.getId().equals(defaultStorageId)).findAny();
            if (!newDefaultStorageOpt.isPresent()) {
                throw new NoSuchStorageException("Storage with id " + defaultStorageId + " is not found");
            }
            final Storage newDefaultStorage = newDefaultStorageOpt.get();
            for (final Storage s : allStorages) {
                if (s.isDefault() && !newDefaultStorage.getId().equals(s.getId())) {
                    storageDbRepository.setDefaultStorage(s.getId(), false);
                }
            }
            storageDbRepository.setDefaultStorage(newDefaultStorage.getId(), true);
            return storageDbRepository.getStorage(newDefaultStorage.getId());
        }).get();
    }
}
