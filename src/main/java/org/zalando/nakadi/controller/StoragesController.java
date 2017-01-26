package org.zalando.nakadi.controller;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.Result;
import org.zalando.nakadi.service.StorageService;
import org.zalando.problem.spring.web.advice.Responses;

import static org.springframework.http.ResponseEntity.status;

@RestController
public class StoragesController {

    private final SecuritySettings securitySettings;
    private final StorageService storageService;

    @Autowired
    public StoragesController(final SecuritySettings securitySettings, final StorageService storageService) {
        this.securitySettings = securitySettings;
        this.storageService = storageService;
    }

    @RequestMapping(value = "/storages", method = RequestMethod.GET)
    public ResponseEntity<?> listStorages(final Client client) {
        if (isNotAdmin(client)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        return status(HttpStatus.OK).body(storageService.listStorages());
    }

    @RequestMapping(value = "/storages", method = RequestMethod.POST)
    public ResponseEntity<?> createStorage(@RequestBody final String body,
                                           final NativeWebRequest request,
                                           final Client client) {
        if (isNotAdmin(client)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        final Result<Storage> result = storageService.createStorage(new JSONObject(body));
        if (result.isSuccessful()) {
            return status(HttpStatus.OK).body(result.getValue());
        }
        return Responses.create(result.getProblem(), request);
    }

    @RequestMapping(value = "/storages/{id}", method = RequestMethod.GET)
    public ResponseEntity<?> getStorage(@PathVariable("id") final String id, final NativeWebRequest request,
                                        final Client client) {
        if (isNotAdmin(client)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        final Result<Storage> result = storageService.getStorage(id);
        if (result.isSuccessful()) {
            return status(HttpStatus.OK).body(result.getValue());
        }
        return Responses.create(result.getProblem(), request);
    }

    @RequestMapping(value = "/storages/{id}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteStorage(@PathVariable("id") final String id, final NativeWebRequest request,
                                           final Client client) {
        if (isNotAdmin(client)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        final Result<Void> result = storageService.deleteStorage(id);
        if (result.isSuccessful()) {
            return status(HttpStatus.OK).build();
        }
        return Responses.create(result.getProblem(), request);
    }

    private boolean isNotAdmin(final Client client) {
        return !client.getClientId().equals(securitySettings.getAdminClientId());
    }
}
