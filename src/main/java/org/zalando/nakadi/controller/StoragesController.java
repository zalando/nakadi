package org.zalando.nakadi.controller;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.ForbiddenOperationException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.exceptions.runtime.UnknownStorageTypeException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.StorageService;

import java.util.List;

import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;

@RestController
public class StoragesController {

    private final StorageService storageService;
    private final AdminService adminService;

    @Autowired
    public StoragesController(final StorageService storageService, final AdminService adminService) {
        this.storageService = storageService;
        this.adminService = adminService;
    }

    @RequestMapping(value = "/storages", method = RequestMethod.GET)
    public ResponseEntity<?> listStorages(final NativeWebRequest request)
            throws InternalNakadiException, PluginException, ForbiddenOperationException {
        if (!adminService.isAdmin(AuthorizationService.Operation.READ)) {
            throw new ForbiddenOperationException("Admin privileges required to perform this operation");
        }
        final List<Storage> storages = storageService.listStorages();
        return status(OK).body(storages);
    }

    @RequestMapping(value = "/storages", method = RequestMethod.POST)
    public ResponseEntity<?> createStorage(@RequestBody final String storage,
                                           final NativeWebRequest request)
            throws PluginException, DbWriteOperationsBlockedException,
            DuplicatedStorageException, InternalNakadiException, UnknownStorageTypeException,
            ForbiddenOperationException {
        if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
            throw new ForbiddenOperationException("Admin privileges required to perform this operation");
        }
        storageService.createStorage(new JSONObject(storage));
        return status(CREATED).build();
    }

    @RequestMapping(value = "/storages/{id}", method = RequestMethod.GET)
    public ResponseEntity<?> getStorage(@PathVariable("id") final String id, final NativeWebRequest request)
            throws PluginException, NoSuchStorageException, InternalNakadiException, ForbiddenOperationException {
        if (!adminService.isAdmin(AuthorizationService.Operation.READ)) {
            throw new ForbiddenOperationException("Admin privileges required to perform this operation");
        }
        final Storage storage = storageService.getStorage(id);
        return status(OK).body(storage);
    }

    @RequestMapping(value = "/storages/{id}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteStorage(@PathVariable("id") final String id, final NativeWebRequest request)
            throws PluginException, DbWriteOperationsBlockedException, NoSuchStorageException,
            StorageIsUsedException, InternalNakadiException, ForbiddenOperationException {
        if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
            throw new ForbiddenOperationException("Admin privileges required to perform this operation");
        }
        storageService.deleteStorage(id);
        return status(NO_CONTENT).build();
    }

    @RequestMapping(value = "/storages/default/{id}", method = RequestMethod.PUT)
    public ResponseEntity<?> setDefaultStorage(@PathVariable("id") final String id, final NativeWebRequest request)
            throws PluginException, NoSuchStorageException, InternalNakadiException, ForbiddenOperationException {
        if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
            throw new ForbiddenOperationException("Admin privileges required to perform this operation");
        }
        final Storage storage = storageService.setDefaultStorage(id);
        return status(OK).body(storage);
    }
}
