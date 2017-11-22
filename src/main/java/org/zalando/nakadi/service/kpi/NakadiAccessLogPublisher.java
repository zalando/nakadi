package org.zalando.nakadi.service.kpi;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.util.FeatureToggleService;

@Service
public class NakadiAccessLogPublisher extends AbstractNakadiPublisher {

    private final String nakadiAccessLogEventType;

    @Autowired
    public NakadiAccessLogPublisher(
            final FeatureToggleService featureToggleService,
            final NakadiPublisher publisher,
            @Value("${nakadi.kpi.event-types.nakadi.access.log:nakadi.access.log}") final String etName) {
        super(featureToggleService, publisher);
        this.nakadiAccessLogEventType = etName;
    }

    public void publish(final String method, final String path, final String query,
                        final String user, final int status, final Long timing) {
        publish(nakadiAccessLogEventType, method,path, query,user, status, timing);
    }

    @Override
    protected JSONObject prepareData(Object[] data) {
        return new JSONObject()
                .put("method", data[0])
                .put("path", data[1])
                .put("query", data[2])
                .put("app", data[3])
                .put("status_code", data[4])
                .put("response_time_ms", data[5]);
    }
}
