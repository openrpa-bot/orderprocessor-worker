package com.nigam.temporal;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.List;

@JsonDeserialize(using = GreetingInput.Deserializer.class)
public class GreetingInput {
    private String name;

    public GreetingInput() {
    }

    public GreetingInput(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static class Deserializer extends JsonDeserializer<GreetingInput> {
        @Override
        public GreetingInput deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonToken token = p.getCurrentToken();
            
            // Handle array format: ["Alice"]
            if (token == JsonToken.START_ARRAY) {
                @SuppressWarnings("unchecked")
                List<String> list = (List<String>) p.getCodec().readValue(p, List.class);
                String value = (list != null && !list.isEmpty()) ? list.get(0) : "World";
                return new GreetingInput(value);
            }
            // Handle string format: "Alice"
            else if (token == JsonToken.VALUE_STRING) {
                String value = p.getValueAsString();
                return new GreetingInput(value != null && !value.isEmpty() ? value : "World");
            }
            // Fallback
            else {
                String value = p.getValueAsString();
                return new GreetingInput(value != null && !value.isEmpty() ? value : "World");
            }
        }
    }
}
