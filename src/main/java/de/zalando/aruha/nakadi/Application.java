package de.zalando.aruha.nakadi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    public static void main(final String[] args) {
        FlowIdUtils.push(FlowIdUtils.generateFlowId());
        try {
            SpringApplication.run(Application.class, args);
        } finally {
            FlowIdUtils.clear();
        }
    }
}
