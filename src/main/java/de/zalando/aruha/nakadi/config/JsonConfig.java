package de.zalando.aruha.nakadi.config;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectDeserializer;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectSerializer;
import org.json.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.zalando.problem.ProblemModule;

import java.io.IOException;

import static com.fasterxml.jackson.databind.PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES;

@Configuration
public class JsonConfig {

    @Bean
    @Primary
    public ObjectMapper jacksonObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper()
                .setPropertyNamingStrategy(CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        SimpleModule jsonObjectModule = new SimpleModule();
        jsonObjectModule.addSerializer(JSONObject.class, new JSONObjectSerializer());
        jsonObjectModule.addDeserializer(JSONObject.class, new JSONObjectDeserializer());

        objectMapper.registerModule(jsonObjectModule);
        objectMapper.registerModule(enumModule());
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new ProblemModule());

        return objectMapper;
    }

    private SimpleModule enumModule() {
        // see http://stackoverflow.com/questions/24157817/jackson-databind-enum-case-insensitive
        final SimpleModule enumModule = new SimpleModule();
        enumModule.setDeserializerModifier(new BeanDeserializerModifier() {
            @Override
            public JsonDeserializer<Enum> modifyEnumDeserializer(DeserializationConfig config,
                                                                 final JavaType type,
                                                                 BeanDescription beanDesc,
                                                                 final JsonDeserializer<?> deserializer) {
                return new LowerCaseEnumJsonDeserializer(type);
            }
        });
        enumModule.addSerializer(Enum.class, new LowerCaseEnumJsonSerializer());
        return enumModule;
    }

    private static class LowerCaseEnumJsonDeserializer extends JsonDeserializer<Enum> {
        private final JavaType type;

        public LowerCaseEnumJsonDeserializer(final JavaType type) {
            this.type = type;
        }

        @Override
        public Enum deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            Class<? extends Enum> rawClass = (Class<Enum<?>>) type.getRawClass();
            return Enum.valueOf(rawClass, jp.getValueAsString().toUpperCase());
        }
    }

    private static class LowerCaseEnumJsonSerializer extends StdSerializer<Enum> {
        public LowerCaseEnumJsonSerializer() {
            super(Enum.class);
        }

        @Override
        public void serialize(Enum value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeString(value.name().toLowerCase());
        }
    }
}
