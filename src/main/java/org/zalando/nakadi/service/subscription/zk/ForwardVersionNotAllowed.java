package org.zalando.nakadi.service.subscription.zk;

import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;

public class ForwardVersionNotAllowed extends MyNakadiRuntimeException1 {
    public ForwardVersionNotAllowed() {
        super("Subscription was migrated from old implementation to new one, this exception may occur for several " +
                "seconds while featuretoggling all nodes");
    }
}
