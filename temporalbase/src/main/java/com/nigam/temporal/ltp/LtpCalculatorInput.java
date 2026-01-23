package com.nigam.temporal.ltp;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@JsonDeserialize(using = LtpCalculatorInput.Deserializer.class)
public class LtpCalculatorInput {
    private String serverName;
    private String serverIP;
    private String port;
    private String apiKey;
    private String indexName;
    private String exchange;
    private String expiry;
    private Integer strikeRange;

    public LtpCalculatorInput() {
    }

    public LtpCalculatorInput(String serverName, String serverIP, String port, String apiKey, String indexName, String exchange, String expiry, Integer strikeRange) {
        this.serverName = serverName;
        this.serverIP = serverIP;
        this.port = port;
        this.apiKey = apiKey;
        this.indexName = indexName;
        this.exchange = exchange;
        this.expiry = expiry;
        this.strikeRange = strikeRange;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getServerIP() {
        return serverIP;
    }

    public void setServerIP(String serverIP) {
        this.serverIP = serverIP;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getExpiry() {
        return expiry;
    }

    public void setExpiry(String expiry) {
        this.expiry = expiry;
    }

    public Integer getStrikeRange() {
        return strikeRange;
    }

    public void setStrikeRange(Integer strikeRange) {
        this.strikeRange = strikeRange;
    }

    public static class Deserializer extends JsonDeserializer<LtpCalculatorInput> {
        @Override
        public LtpCalculatorInput deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonToken token = p.getCurrentToken();
            
            // Handle array format: ["serverName", "serverIP", "port", "apiKey", "indexName", "exchange", "expiry", "strikeRange"]
            if (token == JsonToken.START_ARRAY) {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) p.getCodec().readValue(p, List.class);
                if (list != null && list.size() >= 5) {
                    return new LtpCalculatorInput(
                        String.valueOf(list.get(0)),
                        String.valueOf(list.get(1)),
                        String.valueOf(list.get(2)),
                        String.valueOf(list.get(3)),
                        String.valueOf(list.get(4)),
                        list.size() > 5 ? String.valueOf(list.get(5)) : "NSE_INDEX",
                        list.size() > 6 ? String.valueOf(list.get(6)) : null,
                        list.size() > 7 ? (list.get(7) instanceof Number ? ((Number) list.get(7)).intValue() : 10) : 10
                    );
                }
            }
            // Handle object format: {"serverName": "...", "serverIP": "...", ...}
            else if (token == JsonToken.START_OBJECT) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) p.getCodec().readValue(p, Map.class);
                if (map != null) {
                    Object strikeRangeObj = map.getOrDefault("strikeRange", 10);
                    Integer strikeRange = strikeRangeObj instanceof Number ? ((Number) strikeRangeObj).intValue() : 10;
                    
                    return new LtpCalculatorInput(
                        String.valueOf(map.getOrDefault("serverName", "")),
                        String.valueOf(map.getOrDefault("serverIP", "")),
                        String.valueOf(map.getOrDefault("port", "")),
                        String.valueOf(map.getOrDefault("apiKey", "")),
                        String.valueOf(map.getOrDefault("indexName", "")),
                        String.valueOf(map.getOrDefault("exchange", "NSE_INDEX")),
                        String.valueOf(map.getOrDefault("expiry", "")),
                        strikeRange
                    );
                }
            }
            
            // Fallback to empty input
            return new LtpCalculatorInput("", "", "", "", "", "NSE_INDEX", null, 10);
        }
    }
}
