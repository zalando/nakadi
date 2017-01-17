package org.zalando.nakadi.stream;

public class NakadiStreamFactory {

    public static NakadiStream create() {
        return new NakadiKafkaStream();
    }
}
