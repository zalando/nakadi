package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;

public class EventMetadataTestStub extends MetadataService {

    public EventMetadataTestStub() {
        super(null);
    }

    @Override
    public void enrich(final JSONObject event) {
    }
}
