package com.nigam.temporal.ltp;

import com.google.gson.JsonObject;
import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class LtpCalculatorActivitiesImpl implements LtpCalculatorActivities {
  
  private static JedisPool jedisPool = null;
  
  private JedisPool getJedisPool() {
    if (jedisPool == null) {
      // Get Redis connection details from environment variables
      String redisHost = System.getenv().getOrDefault("REDIS_HOST", "localhost");
      int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
      String redisPassword = System.getenv().getOrDefault("REDIS_PASSWORD", null);
      
      JedisPoolConfig poolConfig = new JedisPoolConfig();
      poolConfig.setMaxTotal(10);
      poolConfig.setMaxIdle(5);
      poolConfig.setMinIdle(1);
      
      if (redisPassword != null && !redisPassword.isEmpty()) {
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000, redisPassword);
      } else {
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000);
      }
      
      System.out.println("✅ Redis connection pool created: " + redisHost + ":" + redisPort);
    }
    return jedisPool;
  }
  
  @Override
  public String fetchOptionChain(String serverName, String serverIP, String port, String apiKey, String indexName, String exchange, String expiry, Integer strikeRange) {
    try {
      // Build the host URL
      String hostUrl = "http://" + serverIP + ":" + port;
      
      // Create OpenAlgo client with custom host
      Object client = createOpenAlgoClient(apiKey, hostUrl);
      
      // Call optionchain API with parameters from input
      // Parameters: symbol, exchange, expiry, strikeRange
      JsonObject response = callOptionChainMethod(client, indexName, exchange, expiry, strikeRange);
      
      // Extract and format response
      String status = response.has("status") ? response.get("status").getAsString() : "unknown";
      String underlying = response.has("underlying") ? response.get("underlying").getAsString() : "unknown";
      int atmStrike = response.has("atm_strike") ? response.get("atm_strike").getAsInt() : 0;
      
      // Log full response for debugging
      System.out.println("Full API Response: " + response.toString());
      
      // Store response in Redis
      String redisKey = buildRedisKey(serverName, indexName, expiry);
      storeInRedis(redisKey, response);
      System.out.println("✅ Stored response in Redis with key: " + redisKey);
      
      // Check if there's an error message
      if (response.has("error") || response.has("message")) {
        String errorMsg = response.has("error") ? response.get("error").getAsString() : 
                          response.has("message") ? response.get("message").getAsString() : "Unknown error";
        return String.format("Error: %s (Status: %s)", errorMsg, status);
      }
      
      // Return formatted result
      return String.format("Status: %s, Underlying: %s, ATM Strike: %d", status, underlying, atmStrike);
      
    } catch (Exception e) {
      System.err.println("Error fetching option chain: " + e.getMessage());
      e.printStackTrace();
      return "Error: " + e.getMessage();
    }
  }
  
  private String buildRedisKey(String serverName, String indexName, String expiry) {
    // Format: openalgo:Angel:IndexName:expiry
    return String.format("openalgo:%s:%s:%s", serverName, indexName, expiry);
  }
  
  private void storeInRedis(String key, JsonObject response) {
    try (Jedis jedis = getJedisPool().getResource()) {
      // Convert JsonObject to JSON string
      Gson gson = new Gson();
      String jsonString = gson.toJson(response);
      
      // Store in Redis
      jedis.set(key, jsonString);
      System.out.println("✅ Stored in Redis - Key: " + key + ", Value length: " + jsonString.length() + " chars");
    } catch (Exception e) {
      System.err.println("⚠️ Failed to store in Redis: " + e.getMessage());
      e.printStackTrace();
      // Don't throw - continue even if Redis fails
    }
  }
  
  private Object createOpenAlgoClient(String apiKey, String hostUrl) throws Exception {
    try {
      // Try to load OpenAlgo class - adjust package name if needed
      Class<?> openAlgoClass = Class.forName("in.openalgo.OpenAlgo");
      
      // Try constructor with (apiKey, hostUrl)
      try {
        java.lang.reflect.Constructor<?> constructor = openAlgoClass.getConstructor(String.class, String.class);
        return constructor.newInstance(apiKey, hostUrl);
      } catch (NoSuchMethodException e) {
        // Try constructor with just apiKey
        java.lang.reflect.Constructor<?> constructor = openAlgoClass.getConstructor(String.class);
        return constructor.newInstance(apiKey);
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("OpenAlgo class not found. Please check the package name and ensure the dependency is correctly added.", e);
    }
  }
  
  private JsonObject callOptionChainMethod(Object client, String symbol, String exchange, String expiry, Integer strikeRange) throws Exception {
    Class<?> clientClass = client.getClass();
    
    // Use the correct signature: optionchain(String, String, String, Integer)
    java.lang.reflect.Method method = clientClass.getMethod("optionchain", String.class, String.class, String.class, Integer.class);
    Object result = method.invoke(client, symbol, exchange, expiry, strikeRange);
    
    // Convert result to JsonObject
    if (result instanceof JsonObject) {
      return (JsonObject) result;
    } else {
      // Convert using Gson if needed
      com.google.gson.Gson gson = new com.google.gson.Gson();
      return gson.toJsonTree(result).getAsJsonObject();
    }
  }
}
