package com.nigam.temporal.ltp;

import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LtpCalculatorActivitiesImpl implements LtpCalculatorActivities {
  
  private static JedisPool jedisPool = null;
  private static Connection dbConnection = null;
  
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
      
      // Enhance response with Greeks data for all CE and PE options
      if (response.has("chain") && response.get("chain").isJsonArray()) {
        System.out.println("🔄 Enhancing response with Greeks data...");
        enhanceResponseWithGreeks(client, response, exchange);
        System.out.println("✅ Response enhanced with Greeks data");
      }
      
      // Store enhanced response in Redis
      String redisKey = buildRedisKey(serverName, indexName, expiry);
      storeInRedis(redisKey, response);
      System.out.println("✅ Stored enhanced response in Redis with key: " + redisKey);
      
      // Store chain data row by row in database
      if (response.has("chain") && response.get("chain").isJsonArray()) {
        System.out.println("💾 Storing chain data in database...");
        storeChainInDatabase(serverName, indexName, expiry, response);
        System.out.println("✅ Chain data stored in database");
      }
      
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
  
  private void enhanceResponseWithGreeks(Object client, JsonObject response, String exchange) {
    try {
      if (!response.has("chain") || !response.get("chain").isJsonArray()) {
        System.out.println("⚠️ No chain array found in response");
        return;
      }
      
      com.google.gson.JsonArray chain = response.getAsJsonArray("chain");
      int totalOptions = 0;
      int successCount = 0;
      int errorCount = 0;
      
      System.out.println("🔄 Processing " + chain.size() + " chain entries for Greeks data...");
      
      for (int i = 0; i < chain.size(); i++) {
        com.google.gson.JsonObject chainEntry = chain.get(i).getAsJsonObject();
        
        // Process CE option
        if (chainEntry.has("ce") && chainEntry.get("ce").isJsonObject()) {
          com.google.gson.JsonObject ce = chainEntry.getAsJsonObject("ce");
          if (ce.has("symbol") && !ce.get("symbol").isJsonNull()) {
            String ceSymbol = ce.get("symbol").getAsString();
            totalOptions++;
            System.out.println("  📞 Fetching Greeks for CE: " + ceSymbol);
            try {
              JsonObject greeksResponse = callOptionGreeksMethod(client, ceSymbol, exchange);
              System.out.println("  📥 Received Greeks response for " + ceSymbol);
              addGreeksToOption(ce, greeksResponse);
              successCount++;
              
              // Add 500ms delay between calls to avoid rate limiting
              Thread.sleep(500);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              System.err.println("⚠️ Thread interrupted while waiting between Greeks calls");
              errorCount++;
            } catch (Exception e) {
              System.err.println("⚠️ Failed to fetch Greeks for CE: " + ceSymbol + " - " + e.getMessage());
              e.printStackTrace();
              errorCount++;
            }
          }
        }
        
        // Process PE option
        if (chainEntry.has("pe") && chainEntry.get("pe").isJsonObject()) {
          com.google.gson.JsonObject pe = chainEntry.getAsJsonObject("pe");
          if (pe.has("symbol") && !pe.get("symbol").isJsonNull()) {
            String peSymbol = pe.get("symbol").getAsString();
            totalOptions++;
            System.out.println("  📞 Fetching Greeks for PE: " + peSymbol);
            try {
              JsonObject greeksResponse = callOptionGreeksMethod(client, peSymbol, exchange);
              System.out.println("  📥 Received Greeks response for " + peSymbol);
              addGreeksToOption(pe, greeksResponse);
              successCount++;
              
              // Add 500ms delay between calls to avoid rate limiting
              Thread.sleep(500);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              System.err.println("⚠️ Thread interrupted while waiting between Greeks calls");
              errorCount++;
            } catch (Exception e) {
              System.err.println("⚠️ Failed to fetch Greeks for PE: " + peSymbol + " - " + e.getMessage());
              e.printStackTrace();
              errorCount++;
            }
          }
        }
      }
      
      System.out.println("📊 Greeks enhancement complete: " + successCount + "/" + totalOptions + " successful, " + errorCount + " errors");
    } catch (Exception e) {
      System.err.println("⚠️ Error enhancing response with Greeks: " + e.getMessage());
      e.printStackTrace();
      // Don't throw - continue even if Greeks enhancement fails
    }
  }
  
  private JsonObject callOptionGreeksMethod(Object client, String symbol, String exchange) throws Exception {
    Class<?> clientClass = client.getClass();
    
    // Map exchange for Greeks API (Greeks API uses NFO, BFO, CDS, MCX instead of NSE_INDEX, etc.)
    String greeksExchange = mapExchangeForGreeks(exchange);
    
    // Try optiongreeks(String, String) signature
    try {
      java.lang.reflect.Method method = clientClass.getMethod("optiongreeks", String.class, String.class);
      Object result = method.invoke(client, symbol, greeksExchange);
      
      // Convert result to JsonObject
      JsonObject greeksResponse;
      if (result instanceof JsonObject) {
        greeksResponse = (JsonObject) result;
      } else {
        com.google.gson.Gson gson = new com.google.gson.Gson();
        greeksResponse = gson.toJsonTree(result).getAsJsonObject();
      }
      
      // Log the actual response for debugging
      System.out.println("  🔍 Greeks API response for " + symbol + " (exchange: " + greeksExchange + "): " + greeksResponse.toString());
      
      return greeksResponse;
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("optiongreeks method not found with signature (String, String)", e);
    }
  }
  
  private Connection getDbConnection() throws Exception {
    if (dbConnection == null || dbConnection.isClosed()) {
      // Get database connection details from environment variables
      String dbHost = System.getenv().getOrDefault("DB_HOST", "localhost");
      String dbPort = System.getenv().getOrDefault("DB_PORT", "5432");
      String dbName = System.getenv().getOrDefault("DB_NAME", "pgdb");
      String dbUser = System.getenv().getOrDefault("DB_USER", "pguser");
      String dbPassword = System.getenv().getOrDefault("DB_PASSWORD", "pgpass");
      
      // PostgreSQL/Citus connection URL
      String dbUrl = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName;
      
      dbConnection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
      System.out.println("✅ Database connection established: " + dbHost + ":" + dbPort + "/" + dbName);
      
      // Create table if it doesn't exist
      createTableIfNotExists();
    }
    return dbConnection;
  }
  
  private void createTableIfNotExists() {
    try {
      // PostgreSQL/Citus compatible table creation
      String createTableSql = "CREATE TABLE IF NOT EXISTS openalgo_optionchain (" +
          "id BIGSERIAL PRIMARY KEY, " +
          "server_name VARCHAR(100) NOT NULL, " +
          "underlying VARCHAR(50) NOT NULL, " +
          "underlying_ltp NUMERIC(15, 2), " +
          "underlying_prev_close NUMERIC(15, 2), " +
          "expiry_date VARCHAR(20) NOT NULL, " +
          "atm_strike INTEGER, " +
          "strike INTEGER NOT NULL, " +
          // CE Option fields
          "ce_symbol VARCHAR(100), " +
          "ce_label VARCHAR(20), " +
          "ce_ltp NUMERIC(15, 2), " +
          "ce_bid NUMERIC(15, 2), " +
          "ce_ask NUMERIC(15, 2), " +
          "ce_open NUMERIC(15, 2), " +
          "ce_high NUMERIC(15, 2), " +
          "ce_low NUMERIC(15, 2), " +
          "ce_prev_close NUMERIC(15, 2), " +
          "ce_volume BIGINT, " +
          "ce_oi BIGINT, " +
          "ce_spot_price NUMERIC(15, 2), " +
          "ce_option_price NUMERIC(15, 2), " +
          "ce_implied_volatility NUMERIC(10, 4), " +
          "ce_days_to_expiry NUMERIC(10, 2), " +
          "ce_delta NUMERIC(10, 6), " +
          "ce_gamma NUMERIC(10, 6), " +
          "ce_theta NUMERIC(10, 6), " +
          "ce_vega NUMERIC(10, 6), " +
          // PE Option fields
          "pe_symbol VARCHAR(100), " +
          "pe_label VARCHAR(20), " +
          "pe_ltp NUMERIC(15, 2), " +
          "pe_bid NUMERIC(15, 2), " +
          "pe_ask NUMERIC(15, 2), " +
          "pe_open NUMERIC(15, 2), " +
          "pe_high NUMERIC(15, 2), " +
          "pe_low NUMERIC(15, 2), " +
          "pe_prev_close NUMERIC(15, 2), " +
          "pe_volume BIGINT, " +
          "pe_oi BIGINT, " +
          "pe_spot_price NUMERIC(15, 2), " +
          "pe_option_price NUMERIC(15, 2), " +
          "pe_implied_volatility NUMERIC(10, 4), " +
          "pe_days_to_expiry NUMERIC(10, 2), " +
          "pe_delta NUMERIC(10, 6), " +
          "pe_gamma NUMERIC(10, 6), " +
          "pe_theta NUMERIC(10, 6), " +
          "pe_vega NUMERIC(10, 6), " +
          // Common fields
          "lotsize INTEGER, " +
          "tick_size NUMERIC(10, 2), " +
          "datetime TIMESTAMP NOT NULL" +
          ")";
      
      try (java.sql.Statement stmt = dbConnection.createStatement()) {
        stmt.execute(createTableSql);
        System.out.println("✅ Table 'openalgo_optionchain' created or already exists");
        
        // Create indexes separately (PostgreSQL syntax)
        createIndexIfNotExists("idx_server_underlying_expiry", "openalgo_optionchain", "server_name, underlying, expiry_date");
        createIndexIfNotExists("idx_datetime", "openalgo_optionchain", "datetime");
        createIndexIfNotExists("idx_strike", "openalgo_optionchain", "strike");
        createIndexIfNotExists("idx_ce_symbol", "openalgo_optionchain", "ce_symbol");
        createIndexIfNotExists("idx_pe_symbol", "openalgo_optionchain", "pe_symbol");
      }
    } catch (Exception e) {
      System.err.println("⚠️ Failed to create table: " + e.getMessage());
      e.printStackTrace();
      // Don't throw - continue even if table creation fails (might already exist)
    }
  }
  
  private void createIndexIfNotExists(String indexName, String tableName, String columns) {
    try {
      String createIndexSql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + tableName + " (" + columns + ")";
      try (java.sql.Statement stmt = dbConnection.createStatement()) {
        stmt.execute(createIndexSql);
      }
    } catch (Exception e) {
      System.err.println("⚠️ Failed to create index " + indexName + ": " + e.getMessage());
      // Don't throw - continue even if index creation fails
    }
  }
  
  private void storeChainInDatabase(String serverName, String indexName, String expiry, JsonObject response) {
    try {
      Connection conn = getDbConnection();
      
      // Get current datetime with minute precision
      LocalDateTime now = LocalDateTime.now();
      now = now.withSecond(0).withNano(0); // Round to minute
      Timestamp timestamp = Timestamp.valueOf(now);
      
      // Extract chain data
      JsonArray chain = response.getAsJsonArray("chain");
      String underlying = response.has("underlying") ? response.get("underlying").getAsString() : indexName;
      double underlyingLtp = response.has("underlying_ltp") ? response.get("underlying_ltp").getAsDouble() : 0.0;
      double underlyingPrevClose = response.has("underlying_prev_close") ? response.get("underlying_prev_close").getAsDouble() : 0.0;
      int atmStrike = response.has("atm_strike") ? response.get("atm_strike").getAsInt() : 0;
      
      // Prepare insert statement - CE and PE in single row
      String insertSql = "INSERT INTO openalgo_optionchain (" +
          "server_name, underlying, underlying_ltp, underlying_prev_close, expiry_date, atm_strike, strike, " +
          // CE fields
          "ce_symbol, ce_label, ce_ltp, ce_bid, ce_ask, ce_open, ce_high, ce_low, ce_prev_close, " +
          "ce_volume, ce_oi, ce_spot_price, ce_option_price, ce_implied_volatility, ce_days_to_expiry, " +
          "ce_delta, ce_gamma, ce_theta, ce_vega, " +
          // PE fields
          "pe_symbol, pe_label, pe_ltp, pe_bid, pe_ask, pe_open, pe_high, pe_low, pe_prev_close, " +
          "pe_volume, pe_oi, pe_spot_price, pe_option_price, pe_implied_volatility, pe_days_to_expiry, " +
          "pe_delta, pe_gamma, pe_theta, pe_vega, " +
          // Common fields
          "lotsize, tick_size, datetime" +
          ") VALUES (?, ?, ?, ?, ?, ?, ?, " +
          "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
          "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
          "?, ?, ?)";
      
      PreparedStatement pstmt = conn.prepareStatement(insertSql);
      
      int insertedRows = 0;
      
      for (int i = 0; i < chain.size(); i++) {
        com.google.gson.JsonObject chainEntry = chain.get(i).getAsJsonObject();
        int strike = chainEntry.has("strike") ? chainEntry.get("strike").getAsInt() : 0;
        
        // Get CE and PE options
        com.google.gson.JsonObject ce = null;
        com.google.gson.JsonObject pe = null;
        
        if (chainEntry.has("ce") && chainEntry.get("ce").isJsonObject()) {
          ce = chainEntry.getAsJsonObject("ce");
        }
        if (chainEntry.has("pe") && chainEntry.get("pe").isJsonObject()) {
          pe = chainEntry.getAsJsonObject("pe");
        }
        
        // Insert both CE and PE in single row
        insertStrikeRow(pstmt, serverName, underlying, underlyingLtp, underlyingPrevClose, expiry, 
                       atmStrike, strike, ce, pe, timestamp);
        insertedRows++;
      }
      
      pstmt.executeBatch();
      pstmt.close();
      
      System.out.println("  ✅ Inserted " + insertedRows + " rows into database");
      
    } catch (Exception e) {
      System.err.println("⚠️ Failed to store chain in database: " + e.getMessage());
      e.printStackTrace();
      // Don't throw - continue even if database fails
    }
  }
  
  private void insertStrikeRow(PreparedStatement pstmt, String serverName, String underlying, 
                               double underlyingLtp, double underlyingPrevClose, String expiry, 
                               int atmStrike, int strike, 
                               com.google.gson.JsonObject ce, com.google.gson.JsonObject pe, 
                               Timestamp timestamp) throws Exception {
    int[] paramIndex = {1}; // Use array to pass by reference
    
    // Common fields
    pstmt.setString(paramIndex[0]++, serverName);
    pstmt.setString(paramIndex[0]++, underlying);
    pstmt.setDouble(paramIndex[0]++, underlyingLtp);
    pstmt.setDouble(paramIndex[0]++, underlyingPrevClose);
    pstmt.setString(paramIndex[0]++, expiry);
    pstmt.setInt(paramIndex[0]++, atmStrike);
    pstmt.setInt(paramIndex[0]++, strike);
    
    // CE Option fields
    setOptionFields(pstmt, paramIndex, ce);
    
    // PE Option fields
    setOptionFields(pstmt, paramIndex, pe);
    
    // Common fields (lotsize and tick_size from CE or PE, whichever is available)
    com.google.gson.JsonObject optionForCommon = ce != null ? ce : pe;
    if (optionForCommon != null) {
      pstmt.setInt(paramIndex[0]++, optionForCommon.has("lotsize") ? optionForCommon.get("lotsize").getAsInt() : 0);
      pstmt.setDouble(paramIndex[0]++, optionForCommon.has("tick_size") ? optionForCommon.get("tick_size").getAsDouble() : 0.0);
    } else {
      pstmt.setInt(paramIndex[0]++, 0);
      pstmt.setDouble(paramIndex[0]++, 0.0);
    }
    
    // Datetime
    pstmt.setTimestamp(paramIndex[0]++, timestamp);
    
    pstmt.addBatch();
  }
  
  private void setOptionFields(PreparedStatement pstmt, int[] paramIndex, com.google.gson.JsonObject option) throws Exception {
    
    if (option == null) {
      // Set all fields to null
      for (int i = 0; i < 26; i++) {
        pstmt.setNull(paramIndex[0]++, java.sql.Types.NULL);
      }
      return;
    }
    
    // Option basic fields
    pstmt.setString(paramIndex[0]++, option.has("symbol") ? option.get("symbol").getAsString() : null);
    pstmt.setString(paramIndex[0]++, option.has("label") ? option.get("label").getAsString() : null);
    pstmt.setDouble(paramIndex[0]++, option.has("ltp") ? option.get("ltp").getAsDouble() : 0.0);
    pstmt.setDouble(paramIndex[0]++, option.has("bid") ? option.get("bid").getAsDouble() : 0.0);
    pstmt.setDouble(paramIndex[0]++, option.has("ask") ? option.get("ask").getAsDouble() : 0.0);
    pstmt.setDouble(paramIndex[0]++, option.has("open") ? option.get("open").getAsDouble() : 0.0);
    pstmt.setDouble(paramIndex[0]++, option.has("high") ? option.get("high").getAsDouble() : 0.0);
    pstmt.setDouble(paramIndex[0]++, option.has("low") ? option.get("low").getAsDouble() : 0.0);
    pstmt.setDouble(paramIndex[0]++, option.has("prev_close") ? option.get("prev_close").getAsDouble() : 0.0);
    pstmt.setLong(paramIndex[0]++, option.has("volume") ? option.get("volume").getAsLong() : 0);
    pstmt.setLong(paramIndex[0]++, option.has("oi") ? option.get("oi").getAsLong() : 0);
    
    // Greeks fields (nullable)
    if (option.has("spot_price") && !option.get("spot_price").isJsonNull()) {
      pstmt.setDouble(paramIndex[0]++, option.get("spot_price").getAsDouble());
    } else {
      pstmt.setNull(paramIndex[0]++, java.sql.Types.DOUBLE);
    }
    if (option.has("option_price") && !option.get("option_price").isJsonNull()) {
      pstmt.setDouble(paramIndex[0]++, option.get("option_price").getAsDouble());
    } else {
      pstmt.setNull(paramIndex[0]++, java.sql.Types.DOUBLE);
    }
    if (option.has("implied_volatility") && !option.get("implied_volatility").isJsonNull()) {
      pstmt.setDouble(paramIndex[0]++, option.get("implied_volatility").getAsDouble());
    } else {
      pstmt.setNull(paramIndex[0]++, java.sql.Types.DOUBLE);
    }
    if (option.has("days_to_expiry") && !option.get("days_to_expiry").isJsonNull()) {
      pstmt.setDouble(paramIndex[0]++, option.get("days_to_expiry").getAsDouble());
    } else {
      pstmt.setNull(paramIndex[0]++, java.sql.Types.DOUBLE);
    }
    
    // Greeks object fields
    double delta = 0.0, gamma = 0.0, theta = 0.0, vega = 0.0;
    if (option.has("greeks") && option.get("greeks").isJsonObject()) {
      com.google.gson.JsonObject greeks = option.getAsJsonObject("greeks");
      delta = greeks.has("delta") ? greeks.get("delta").getAsDouble() : 0.0;
      gamma = greeks.has("gamma") ? greeks.get("gamma").getAsDouble() : 0.0;
      theta = greeks.has("theta") ? greeks.get("theta").getAsDouble() : 0.0;
      vega = greeks.has("vega") ? greeks.get("vega").getAsDouble() : 0.0;
    }
    pstmt.setDouble(paramIndex[0]++, delta);
    pstmt.setDouble(paramIndex[0]++, gamma);
    pstmt.setDouble(paramIndex[0]++, theta);
    pstmt.setDouble(paramIndex[0]++, vega);
  }
  
  private String mapExchangeForGreeks(String exchange) {
    // Map optionchain exchange format to optiongreeks exchange format
    // Greeks API expects: NFO, BFO, CDS, MCX
    // Optionchain uses: NSE_INDEX, BSE_INDEX, etc.
    if (exchange == null || exchange.isEmpty()) {
      return "NFO"; // Default to NFO
    }
    
    String upperExchange = exchange.toUpperCase();
    if (upperExchange.contains("NSE") || upperExchange.equals("NSE_INDEX")) {
      return "NFO";
    } else if (upperExchange.contains("BSE") || upperExchange.equals("BSE_INDEX")) {
      return "BFO";
    } else if (upperExchange.contains("CDS")) {
      return "CDS";
    } else if (upperExchange.contains("MCX")) {
      return "MCX";
    } else if (upperExchange.equals("NFO") || upperExchange.equals("BFO") || 
               upperExchange.equals("CDS") || upperExchange.equals("MCX")) {
      // Already in correct format
      return upperExchange;
    }
    
    // Default to NFO for NSE options
    return "NFO";
  }
  
  private void addGreeksToOption(com.google.gson.JsonObject option, JsonObject greeksResponse) {
    try {
      String symbol = option.has("symbol") ? option.get("symbol").getAsString() : "unknown";
      
      // Check if Greeks response has the required data (check for greeks object or key fields)
      boolean hasGreeksData = greeksResponse.has("greeks") || 
                              greeksResponse.has("spot_price") || 
                              greeksResponse.has("option_price") ||
                              greeksResponse.has("delta") ||
                              greeksResponse.has("gamma");
      
      // Check status if present, but don't fail if status is missing
      if (greeksResponse.has("status")) {
        String greeksStatus = greeksResponse.get("status").getAsString();
        if (!"success".equalsIgnoreCase(greeksStatus) && !hasGreeksData) {
          System.err.println("⚠️ Greeks response not successful for symbol: " + symbol + ", status: " + greeksStatus);
          return;
        }
      } else if (!hasGreeksData) {
        // If no status field and no Greeks data, log warning but try to add what we can
        System.err.println("⚠️ Greeks response missing status and data for symbol: " + symbol);
        // Continue anyway - might still have some data
      }
      
      System.out.println("  ✅ Adding Greeks for: " + symbol);
      
      // Add greeks data fields to the option object
      if (greeksResponse.has("spot_price")) {
        option.add("spot_price", greeksResponse.get("spot_price").deepCopy());
        System.out.println("    - Added spot_price: " + greeksResponse.get("spot_price"));
      }
      if (greeksResponse.has("option_price")) {
        option.add("option_price", greeksResponse.get("option_price").deepCopy());
        System.out.println("    - Added option_price: " + greeksResponse.get("option_price"));
      }
      if (greeksResponse.has("implied_volatility")) {
        option.add("implied_volatility", greeksResponse.get("implied_volatility").deepCopy());
        System.out.println("    - Added implied_volatility: " + greeksResponse.get("implied_volatility"));
      }
      if (greeksResponse.has("days_to_expiry")) {
        option.add("days_to_expiry", greeksResponse.get("days_to_expiry").deepCopy());
        System.out.println("    - Added days_to_expiry: " + greeksResponse.get("days_to_expiry"));
      }
      
      // Add greeks object (could be nested or flat)
      if (greeksResponse.has("greeks") && greeksResponse.get("greeks").isJsonObject()) {
        option.add("greeks", greeksResponse.getAsJsonObject("greeks").deepCopy());
        System.out.println("    - Added greeks object");
      } else {
        // If greeks are at root level, add them as a greeks object
        com.google.gson.JsonObject greeksObj = new com.google.gson.JsonObject();
        boolean hasGreeks = false;
        
        if (greeksResponse.has("delta")) {
          greeksObj.add("delta", greeksResponse.get("delta").deepCopy());
          hasGreeks = true;
        }
        if (greeksResponse.has("gamma")) {
          greeksObj.add("gamma", greeksResponse.get("gamma").deepCopy());
          hasGreeks = true;
        }
        if (greeksResponse.has("theta")) {
          greeksObj.add("theta", greeksResponse.get("theta").deepCopy());
          hasGreeks = true;
        }
        if (greeksResponse.has("vega")) {
          greeksObj.add("vega", greeksResponse.get("vega").deepCopy());
          hasGreeks = true;
        }
        
        if (hasGreeks) {
          option.add("greeks", greeksObj);
          System.out.println("    - Added greeks object from root level fields");
        }
      }
    } catch (Exception e) {
      System.err.println("⚠️ Error adding Greeks to option: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
