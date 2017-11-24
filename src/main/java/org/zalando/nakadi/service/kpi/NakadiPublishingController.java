package org.zalando.nakadi.service.kpi;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.service.kpi.publisher.NakadiKpiPublisher;

import java.util.function.Supplier;

@Service
public class NakadiPublishingController {

    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final String accessLogEventType;

    @Autowired
    public NakadiPublishingController(
            final NakadiKpiPublisher nakadiKpiPublisher,
            @Value("${nakadi.kpi.event-types.nakadiAccessLog}") final String accessLogEventType) {
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.accessLogEventType = accessLogEventType;
    }

    public void publishAccessLog(final Supplier<JSONObject> eventSupplier) {
        nakadiKpiPublisher.publish(accessLogEventType, eventSupplier);
    }

}
