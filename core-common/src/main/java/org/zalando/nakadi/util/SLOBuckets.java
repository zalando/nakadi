package org.zalando.nakadi.util;

public class SLOBuckets {
    private static final String BUCKET_NAME_5_KB = "<5K";
    private static final String BUCKET_NAME_5_50_KB = "5K-50K";
    private static final String BUCKET_NAME_MORE_THAN_50_KB = ">50K";

    private static final long BUCKET_5_KB = 5000L;
    private static final long BUCKET_50_KB = 50000L;

    public static String getNameForBatchSize(final long batchSize) {
        if (batchSize > BUCKET_50_KB) {
            return BUCKET_NAME_MORE_THAN_50_KB;
        } else if (batchSize < BUCKET_5_KB) {
            return BUCKET_NAME_5_KB;
        }
        return BUCKET_NAME_5_50_KB;
    }
}
