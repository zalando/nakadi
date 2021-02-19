package org.zalando.nakadi.service.timeline;

public class TimelinesConfig {

    public static final String VERSION_PATH = toZkPath("/state");
    public static final String NODES_PATH = toZkPath("/nodes");

    private static final String ROOT_PATH = "/nakadi/timelines";

    private static String toZkPath(final String path) {
        return ROOT_PATH + path;
    }

    public static String getNodePath(final String nodeId) {
        return toZkPath("/" + nodeId);
    }

}
