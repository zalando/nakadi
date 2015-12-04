package de.zalando.aruha.nakadi.service;

import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import java.io.IOException;

public class EventStream implements Runnable {

    private ResponseBodyEmitter responseEmitter;

    private EventStreamConfig config;

    public EventStream(final ResponseBodyEmitter responseEmitter, final EventStreamConfig config) {
        this.responseEmitter = responseEmitter;
        this.config = config;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Thread.sleep(2000);
                responseEmitter.send("blah\n");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // this happens when connection is closed from client side

            e.printStackTrace();
        }
    }

}
