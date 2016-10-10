package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.FloodService;
import org.zalando.nakadi.util.FeatureToggleService;

@RestController
@RequestMapping(value = "/settings")
public class NakadiController {

    private final FloodService floodService;
    private final FeatureToggleService featureToggleService;
    private final SecuritySettings securitySettings;

    @Autowired
    public NakadiController(final FloodService floodService,
                            final FeatureToggleService featureToggleService,
                            final SecuritySettings securitySettings) {
        this.floodService = floodService;
        this.featureToggleService = featureToggleService;
        this.securitySettings = securitySettings;
    }

    @RequestMapping(path = "/flooders", method = RequestMethod.GET)
    public ResponseEntity<?> getFlooders(final Client client) {
        if (!client.idMatches(securitySettings.getAdminClientId())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        return ResponseEntity.ok(floodService.getFlooders());
    }

    @RequestMapping(value = "/flooders", method = RequestMethod.POST)
    public ResponseEntity blockFlooders(@RequestBody final FloodService.Flooder flooder, final Client client) {
        if (!client.idMatches(securitySettings.getAdminClientId())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        floodService.blockFlooder(flooder);
        return ResponseEntity.noContent().build();
    }

    @RequestMapping(value = "/flooders", method = RequestMethod.DELETE)
    public ResponseEntity unblockFlooder(@RequestBody final FloodService.Flooder flooder, final Client client) {
        if (!client.idMatches(securitySettings.getAdminClientId())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        floodService.unblockFlooder(flooder);
        return ResponseEntity.noContent().build();
    }

    @RequestMapping(path = "/features", method = RequestMethod.GET)
    public ResponseEntity<?> getFeatures(final Client client) {
        if (!client.idMatches(securitySettings.getAdminClientId())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        return ResponseEntity.ok(new ItemsWrapper<>(featureToggleService.getFeatures()));
    }

    @RequestMapping(path = "/features", method = RequestMethod.POST)
    public ResponseEntity<?> setFeature(@RequestBody final FeatureToggleService.FeatureWrapper featureWrapper,
                                        final Client client) {
        if (!client.idMatches(securitySettings.getAdminClientId())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        featureToggleService.setFeature(featureWrapper);
        return ResponseEntity.noContent().build();
    }
}
