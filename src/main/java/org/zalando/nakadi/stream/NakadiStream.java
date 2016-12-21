package org.zalando.nakadi.stream;

import org.zalando.nakadi.stream.expression.Interpreter;

public interface NakadiStream {

    public void stream(StreamConfig streamConfig, Interpreter interpreter);

}
