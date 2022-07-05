package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.NakadiMetadata;

public interface EventOwnerExtractor {

    EventOwnerHeader extractEventOwner(JSONObject event);

    EventOwnerHeader extractEventOwner(NakadiMetadata metadata);
}
