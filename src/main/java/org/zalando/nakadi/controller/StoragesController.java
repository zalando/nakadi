package org.zalando.nakadi.controller;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidStorageConfigurationException;
import org.zalando.nakadi.exceptions.runtime.InvalidStorageTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchStorageException;
import org.zalando.nakadi.exceptions.runtime.StorageIsUsedException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.StorageService;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.ws.rs.core.Response;
import java.util.List;

import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;

@RestController
public class StoragesController {

    private static final Logger LOG = LoggerFactory.getLogger(StoragesController.class);

    private final StorageService storageService;
    private final AdminService adminService;

    @Autowired
    public StoragesController(final StorageService storageService,
                              final AdminService adminService) {
        this.storageService = storageService;
        this.adminService = adminService;
    }

    @RequestMapping(value = "/storages", method = RequestMethod.GET)
    public ResponseEntity<?> listStorages(final NativeWebRequest request) {
        if (!adminService.isAdmin(AuthorizationService.Operation.READ)) {
            return status(FORBIDDEN).build();
        }
        final List<Storage> storages = storageService.listStorages();
        return status(OK).body(storages);
    }

    @RequestMapping(value = "/storages", method = RequestMethod.POST)
    public ResponseEntity<?> createStorage(@RequestBody final String storage,
                                           final NativeWebRequest request) {
        if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
            return status(FORBIDDEN).build();
        }
        storageService.createStorage(new JSONObject(storage));
        return status(CREATED).build();
    }

    @RequestMapping(value = "/storages/{id}", method = RequestMethod.GET)
    public ResponseEntity<?> getStorage(@PathVariable("id") final String id, final NativeWebRequest request) {
        if (!adminService.isAdmin(AuthorizationService.Operation.READ)) {
            return status(FORBIDDEN).build();
        }
        final Storage storage = storageService.getStorage(id);
        return status(OK).body(storage);
    }

    @RequestMapping(value = "/storages/{id}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteStorage(@PathVariable("id") final String id, final NativeWebRequest request) {
        if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
            return status(FORBIDDEN).build();
        }
        storageService.deleteStorage(id);
        return status(NO_CONTENT).build();
    }

    @RequestMapping(value = "/storages/default/{id}", method = RequestMethod.PUT)
    public ResponseEntity<?> setDefaultStorage(@PathVariable("id") final String id, final NativeWebRequest request) {
        if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
            return status(FORBIDDEN).build();
        }
        final Storage storage = storageService.setDefaultStorage(id);
        return status(OK).body(storage);
    }

    @ExceptionHandler(NoSuchStorageException.class)
    public ResponseEntity<Problem> handleNoStorageException(final NoSuchStorageException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(NOT_FOUND, exception.getMessage(), request);
    }

    @ExceptionHandler(InternalNakadiException.class)
    public ResponseEntity<Problem> handleInternalNakadiException(final InternalNakadiException exception,
                                                            final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(INTERNAL_SERVER_ERROR, exception.getMessage(), request);
    }

    @ExceptionHandler(StorageIsUsedException.class)
    public ResponseEntity<Problem> handleStorageIsUsedException(final StorageIsUsedException exception,
                                                                 final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(Response.Status.FORBIDDEN, exception.getMessage(), request);
    }

    @ExceptionHandler(InvalidStorageTypeException.class)
    public ResponseEntity<Problem> handleInvalidStorageTypeException(final InvalidStorageTypeException exception,
                                                                final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(MoreStatus.UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }

    @ExceptionHandler(InvalidStorageConfigurationException.class)
    public ResponseEntity<Problem> handleInvalidStorageConfigurationException(
            final InvalidStorageConfigurationException exception, final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(MoreStatus.UNPROCESSABLE_ENTITY, exception.getMessage(), request);
    }

    @ExceptionHandler(DuplicatedStorageException.class)
    public ResponseEntity<Problem> handleDuplicatedStorageException(final DuplicatedStorageException exception,
                                                                     final NativeWebRequest request) {
        LOG.debug(exception.getMessage(), exception);
        return Responses.create(CONFLICT, exception.getMessage(), request);
    }

}
