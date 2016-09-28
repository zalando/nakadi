package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.service.FloodService;
import org.zalando.nakadi.util.FeatureToggleService;

@RestController
@RequestMapping(value = "/nakadi")
public class NakadiController {

    private final FloodService floodService;
    private final FeatureToggleService featureToggleService;

    @Autowired
    public NakadiController(final FloodService floodService, final FeatureToggleService featureToggleService) {
        this.floodService = floodService;
        this.featureToggleService = featureToggleService;
    }

    @RequestMapping(path = "/flooders", method = RequestMethod.GET)
    public ResponseEntity<?> getFlooders() {
        return ResponseEntity.ok(floodService.getFlooders());
    }

    @RequestMapping(value = "/flooders", method = RequestMethod.POST)
    public ResponseEntity blockFlooders(@RequestBody final FloodService.Flooder flooder) {
        floodService.blockFlooder(flooder);
        return ResponseEntity.noContent().build();
    }

    @RequestMapping(value = "/flooders", method = RequestMethod.DELETE)
    public ResponseEntity unblockFlooder(@RequestBody final FloodService.Flooder flooder) {
        floodService.unblockFlooder(flooder);
        return ResponseEntity.noContent().build();
    }

    @RequestMapping(path = "/features", method = RequestMethod.GET)
    public ResponseEntity<?> getFeatures() {
        return ResponseEntity.ok(new ItemsWrapper<>(featureToggleService.getFeatures()));
    }

    @RequestMapping(path = "/features", method = RequestMethod.POST)
    public ResponseEntity<?> setFeature(@RequestBody final FeatureToggleService.FeatureWrapper featureWrapper) {
        featureToggleService.setFeature(featureWrapper);
        return ResponseEntity.noContent().build();
    }
}
