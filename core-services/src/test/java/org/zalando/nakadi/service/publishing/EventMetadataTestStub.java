package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;

public class EventMetadataTestStub extends EventMetadata {

    public EventMetadataTestStub() {
        super(null);
    }

    @Override
    public JSONObject addTo(final JSONObject event) {
        return event;
    }

    @Override
    public Builder generateMetadata() {
        return new Builder() {
            @Override
            public JSONObject asJson() {
                return null;
            }
        };
    }
}
