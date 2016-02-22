package de.zalando.aruha.nakadi.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectDeserializer;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectSerializer;
import org.json.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.zalando.problem.ProblemModule;

@Configuration
public class JsonConfig {

    @Bean
    @Primary
    public ObjectMapper jacksonObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper().setPropertyNamingStrategy(
                PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        SimpleModule jsonObjectModule = new SimpleModule();
        jsonObjectModule.addSerializer(JSONObject.class, new JSONObjectSerializer());
        jsonObjectModule.addDeserializer(JSONObject.class, new JSONObjectDeserializer());

        objectMapper.registerModule(jsonObjectModule);
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new ProblemModule());

        return objectMapper;
    }

}
