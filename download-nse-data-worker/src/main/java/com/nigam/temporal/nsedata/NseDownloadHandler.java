package com.nigam.temporal.nsedata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;

/**
 * Dispatches NSE download tasks based on workflow input and pushes processed data to Redis and Kafka.
 */
public class NseDownloadHandler {

  private static final Logger log = LogManager.getLogger(NseDownloadHandler.class);

  private static final String NSE_ALL_INDICES_URL = "https://www.nseindia.com/api/allIndices?csv=true";
  private static final String NSE_EQUITY_DATA_URL = "https://www.nseindia.com/api/live-analysis-variations?index=gainers&type=allSec&csv=true";
  private static final String NSE_REFERER_INDICES = "https://www.nseindia.com/market-data/live-market-indices";
  private static final String NSE_REFERER_OPTIONS = "https://www.nseindia.com/option-chain";
  private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36";
  
  /** Common Kafka topic for all NSE data downloads to avoid race conditions at client side */
  private static final String KAFKA_TOPIC_COMMON = "nse.data";
  
  /** Default symbol for option chain */
  private static final String DEFAULT_SYMBOL = "NIFTY";
  
  /** Redis key pattern for expiry dates (includes symbol) */
  private static String getExpiriesDataKey(String symbol) {
    return "nse:optionchain:" + symbol + ":expiries:data";
  }
  
  private static String getExpiriesTimestampKey(String symbol) {
    return "nse:optionchain:" + symbol + ":expiries:timestamp";
  }

  private final NseDataRedisPublisher redisPublisher;
  private final NseDataKafkaPublisher kafkaPublisher;
  private final HttpClient httpClient;

  public NseDownloadHandler(NseDataRedisPublisher redisPublisher, NseDataKafkaPublisher kafkaPublisher) {
    this.redisPublisher = redisPublisher;
    this.kafkaPublisher = kafkaPublisher;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(15))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
  }

  /**
   * Route by taskType and run the matching download; result is published to Redis and Kafka.
   */
  public String handle(DownloadNseDataInput input) {
    System.out.println("🔄 NseDownloadHandler.handle() called - taskType=" + (input != null ? input.getTaskType() : "null"));
    log.info("handle() entered, input={}", input);
    if (input == null || input.getTaskType() == null || input.getTaskType().isEmpty()) {
      System.out.println("❌ NseDownloadHandler: taskType missing or empty");
      log.warn("handle() rejected: taskType missing or empty, input={}", input);
      return "Error: taskType is required";
    }
    String taskType = input.getTaskType().trim().toLowerCase(Locale.ROOT);
    System.out.println("🔄 NseDownloadHandler: routing to taskType=" + taskType);
    log.info("handle() resolved taskType='{}', date='{}', targetPath='{}'", taskType, input.getDate(), input.getTargetPath());

    switch (taskType) {
      case "allindices":
      case "all_indices":
      case "all-indices":
        log.info("handle() dispatching to downloadAllIndices");
        String result = downloadAllIndices(input);
        log.info("handle() downloadAllIndices returned: {}", result);
        return result;
      case "optionchange":
      case "option_change":
      case "option-chain":
      case "optionchain":
        System.out.println("🔄 NseDownloadHandler: dispatching to downloadOptionChain");
        log.info("handle() dispatching to downloadOptionChain");
        String optionResult = downloadOptionChain(input);
        System.out.println("✅ NseDownloadHandler: downloadOptionChain returned: " + optionResult);
        log.info("handle() downloadOptionChain returned: {}", optionResult);
        return optionResult;
      case "equity":
      case "equitydata":
      case "equity_data":
      case "equity-data":
        System.out.println("🔄 NseDownloadHandler: dispatching to downloadEquityData");
        log.info("handle() dispatching to downloadEquityData");
        String equityResult = downloadEquityData(input);
        System.out.println("✅ NseDownloadHandler: downloadEquityData returned: " + equityResult);
        log.info("handle() downloadEquityData returned: {}", equityResult);
        return equityResult;
      // future: case "bhav": return downloadBhav(input);
      // future: case "participant": return downloadParticipant(input);
      // future: case "sec_list": return downloadSecList(input);
      default:
        log.warn("handle() unknown taskType='{}'", input.getTaskType());
        return "Error: unknown taskType=" + input.getTaskType();
    }
  }

  /**
   * First page: GET https://www.nseindia.com/api/allIndices?csv=true
   * Returns CSV; we store raw CSV and parsed summary in Redis/Kafka.
   */
  private String downloadAllIndices(DownloadNseDataInput input) {
    log.info("downloadAllIndices() started, url={}", NSE_ALL_INDICES_URL);
    try {
      // Use taskTimeout from input for NSE API call timeout, default 30 seconds
      int apiTimeoutMs = input.getTaskTimeout() != null ? input.getTaskTimeout() : 30000;
      Duration apiTimeout = Duration.ofMillis(apiTimeoutMs);
      log.info("downloadAllIndices() NSE API timeout set to {}ms", apiTimeoutMs);
      
      log.debug("downloadAllIndices() building HTTP request");
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(NSE_ALL_INDICES_URL))
          .header("Accept", "*/*")
          .header("Accept-Language", "en-US,en;q=0.9,hi;q=0.8")
          .header("Referer", NSE_REFERER_INDICES)
          .header("User-Agent", USER_AGENT)
          .header("X-Requested-With", "XMLHttpRequest")
          .header("sec-ch-ua", "\"Not(A:Brand\";v=\"8\", \"Chromium\";v=\"144\", \"Google Chrome\";v=\"144\"")
          .header("sec-ch-ua-mobile", "?0")
          .header("sec-ch-ua-platform", "\"Windows\"")
          .header("sec-fetch-dest", "empty")
          .header("sec-fetch-mode", "cors")
          .header("sec-fetch-site", "same-origin")
          .GET()
          .timeout(apiTimeout)
          .build();
      log.info("downloadAllIndices() sending HTTP GET with timeout {}ms", apiTimeoutMs);

      HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
      int code = response.statusCode();
      int bodyLength = response.body() != null ? response.body().length : 0;
      log.info("downloadAllIndices() HTTP response: status={}, bodyLength={}", code, bodyLength);
      if (code != 200) {
        log.warn("downloadAllIndices() non-200 status, returning error");
        return "Error: allIndices HTTP " + code;
      }

      String csv = new String(response.body(), StandardCharsets.UTF_8);
      Instant downloadTimestamp = Instant.now();
      String timestampStr = downloadTimestamp.toString();
      int dataSizeBytes = csv.getBytes(StandardCharsets.UTF_8).length;
      int dataSizeChars = csv.length();
      log.info("📥 DOWNLOADED DATA: size={} bytes ({} chars), timestamp={}", dataSizeBytes, dataSizeChars, downloadTimestamp);
      log.info("downloadAllIndices() body decoded, csvLength={}, preview={}", csv.length(), csv.length() > 200 ? csv.substring(0, 200) + "..." : csv);
      
      String taskName = "allIndices";
      String redisKeyBase = "nse:allindices";
      publishDataAndNotify(taskName, csv, timestampStr, redisKeyBase);

      String okMsg = "OK: allIndices downloaded, length=" + csv.length() + ", redisKeys=" + redisKeyBase + ":current:data," + redisKeyBase + ":current:timestamp";
      log.info("downloadAllIndices() completed successfully: {}", okMsg);
      return okMsg;
    } catch (Exception e) {
      log.error("downloadAllIndices() failed: {}", e.getMessage(), e);
      return "Error: allIndices " + e.getMessage();
    }
  }

  /**
   * Download equity data (gainers/losers) from NSE.
   * GET https://www.nseindia.com/api/live-analysis-variations?index=gainers&type=allSec&csv=true
   * Returns CSV; we store raw CSV and timestamp in Redis/Kafka.
   */
  private String downloadEquityData(DownloadNseDataInput input) {
    System.out.println("📊 EQUITY DATA: Starting download");
    log.info("downloadEquityData() started");
    try {
      int apiTimeoutMs = input.getTaskTimeout() != null ? input.getTaskTimeout() : 600000;
      Duration apiTimeout = Duration.ofMillis(apiTimeoutMs);
      HttpClient httpClient = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(30))
          .build();

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(NSE_EQUITY_DATA_URL))
          .header("Accept", "*/*")
          .header("Accept-Language", "en-US,en;q=0.9,hi;q=0.8")
          .header("Referer", NSE_REFERER_INDICES)
          .header("User-Agent", USER_AGENT)
          .header("X-Requested-With", "XMLHttpRequest")
          .header("sec-ch-ua", "\"Not(A:Brand\";v=\"8\", \"Chromium\";v=\"144\", \"Google Chrome\";v=\"144\"")
          .header("sec-ch-ua-mobile", "?0")
          .header("sec-ch-ua-platform", "\"Windows\"")
          .header("sec-fetch-dest", "empty")
          .header("sec-fetch-mode", "cors")
          .header("sec-fetch-site", "same-origin")
          .GET()
          .timeout(apiTimeout)
          .build();
      log.info("downloadEquityData() sending HTTP GET with timeout {}ms, URL={}", apiTimeoutMs, NSE_EQUITY_DATA_URL);
      System.out.println("📊 EQUITY DATA: Request URL: " + NSE_EQUITY_DATA_URL);

      HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
      int code = response.statusCode();
      int bodyLength = response.body() != null ? response.body().length : 0;
      log.info("downloadEquityData() HTTP response: status={}, bodyLength={}", code, bodyLength);
      System.out.println("📊 EQUITY DATA: HTTP response status=" + code + ", bodyLength=" + bodyLength);
      
      if (code != 200) {
        String errorMsg = "Error: equityData HTTP " + code;
        log.warn("downloadEquityData() non-200 status, returning error");
        System.out.println("❌ EQUITY DATA: " + errorMsg);
        return errorMsg;
      }

      String csv = new String(response.body(), StandardCharsets.UTF_8);
      Instant downloadTimestamp = Instant.now();
      String timestampStr = downloadTimestamp.toString();
      int dataSizeBytes = csv.getBytes(StandardCharsets.UTF_8).length;
      int dataSizeChars = csv.length();
      log.info("📥 DOWNLOADED DATA: size={} bytes ({} chars), timestamp={}", dataSizeBytes, dataSizeChars, downloadTimestamp);
      log.info("downloadEquityData() body decoded, csvLength={}, preview={}", csv.length(), csv.length() > 200 ? csv.substring(0, 200) + "..." : csv);
      System.out.println("📊 EQUITY DATA: Downloaded " + dataSizeBytes + " bytes (" + dataSizeChars + " chars) at " + downloadTimestamp);
      
      if (csv == null || csv.trim().isEmpty()) {
        String warnMsg = "Warning: equityData response is empty";
        log.warn("downloadEquityData() {}", warnMsg);
        System.out.println("⚠️ EQUITY DATA: " + warnMsg);
      }
      
      String taskName = "equityData";
      String redisKeyBase = "nse:equitydata";
      publishDataAndNotify(taskName, csv, timestampStr, redisKeyBase);

      String okMsg = "OK: equityData downloaded, length=" + csv.length() + ", redisKeys=" + redisKeyBase + ":current:data," + redisKeyBase + ":current:timestamp";
      log.info("downloadEquityData() completed successfully: {}", okMsg);
      System.out.println("✅ EQUITY DATA: " + okMsg);
      return okMsg;
    } catch (Exception e) {
      log.error("downloadEquityData() failed: {}", e.getMessage(), e);
      System.out.println("❌ EQUITY DATA: Error - " + e.getMessage());
      return "Error: equityData " + e.getMessage();
    }
  }

  /**
   * Download option chain data from NSE.
   * GET https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY
   * Returns JSON; we store raw JSON and timestamp in Redis/Kafka.
   */
  private String downloadOptionChain(DownloadNseDataInput input) {
    // Get symbol from input or use default NIFTY
    String symbol = input.getSymbol() != null && !input.getSymbol().isEmpty() 
        ? input.getSymbol() 
        : DEFAULT_SYMBOL;
    int numberOfExpiry = input.getNumberOfExpiry() != null && input.getNumberOfExpiry() > 0 
        ? input.getNumberOfExpiry() 
        : 1;
    
    System.out.println("🔗 OPTION CHAIN: Starting download, symbol=" + symbol + ", numberOfExpiry=" + numberOfExpiry);
    log.info("downloadOptionChain() started, symbol={}, numberOfExpiry={}", symbol, numberOfExpiry);
    
    // Get or refresh expiry dates (refresh daily on first call)
    String expiryDatesJson = getOrRefreshExpiryDates(input, symbol);
    
    // Extract expiry dates list
    java.util.List<String> expiryDates = extractExpiryDatesList(expiryDatesJson, symbol, numberOfExpiry);
    
    if (expiryDates == null || expiryDates.isEmpty()) {
      String errorMsg = "Error: Could not extract expiry dates for symbol " + symbol;
      System.out.println("❌ OPTION CHAIN: " + errorMsg);
      log.error("downloadOptionChain() {}", errorMsg);
      return errorMsg;
    }
    
    System.out.println("🔗 OPTION CHAIN: Processing " + expiryDates.size() + " expiry dates: " + expiryDates);
    log.info("downloadOptionChain() processing {} expiry dates: {}", expiryDates.size(), expiryDates);
    
    // Download option chain for each expiry date
    java.util.List<String> results = new java.util.ArrayList<>();
    for (int i = 0; i < expiryDates.size(); i++) {
      String expiryDate = expiryDates.get(i);
      System.out.println("🔗 OPTION CHAIN: Processing expiry " + (i + 1) + "/" + expiryDates.size() + ": " + expiryDate);
      log.info("downloadOptionChain() processing expiry {}/{}: {}", i + 1, expiryDates.size(), expiryDate);
      
      String result = downloadOptionChainForExpiry(input, symbol, expiryDate);
      results.add("Expiry " + expiryDate + ": " + result);
      
      // Apply delay between expiry downloads if not the last one
      if (i < expiryDates.size() - 1 && input.getTaskdelay() != null && input.getTaskdelay() > 0) {
        try {
          Thread.sleep(input.getTaskdelay());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    
    String combinedResult = String.join(" | ", results);
    System.out.println("✅ OPTION CHAIN: Completed all expiry dates: " + combinedResult);
    log.info("downloadOptionChain() completed all expiry dates: {}", combinedResult);
    return combinedResult;
  }

  /**
   * Download option chain data for a specific symbol and expiry date.
   */
  private String downloadOptionChainForExpiry(DownloadNseDataInput input, String symbol, String expiryDate) {
    // Format expiry date for URL (convert to DD-MMM-YYYY format like "03-Feb-2026")
    String formattedExpiry = formatExpiryForUrl(expiryDate);
    String optionChainUrl = "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=" + symbol + "&expiry=" + formattedExpiry;
    System.out.println("🔗 OPTION CHAIN: Downloading from URL: " + optionChainUrl);
    System.out.println("🔗 OPTION CHAIN: symbol=" + symbol + ", expiry=" + expiryDate + " (formatted: " + formattedExpiry + ")");
    log.info("downloadOptionChainForExpiry() url={}, symbol={}, expiry={}, formattedExpiry={}", optionChainUrl, symbol, expiryDate, formattedExpiry);
    try {
      // Use taskTimeout from input for NSE API call timeout, default 30 seconds
      int apiTimeoutMs = input.getTaskTimeout() != null ? input.getTaskTimeout() : 30000;
      Duration apiTimeout = Duration.ofMillis(apiTimeoutMs);
      log.info("downloadOptionChain() NSE API timeout set to {}ms", apiTimeoutMs);
      
      log.debug("downloadOptionChain() building HTTP request for URL: {}", optionChainUrl);
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(optionChainUrl))
          .header("Accept", "*/*")
          .header("Accept-Language", "en-US,en;q=0.9,hi;q=0.8")
          .header("Referer", NSE_REFERER_OPTIONS)
          .header("User-Agent", USER_AGENT)
          .header("X-Requested-With", "XMLHttpRequest")
          .header("sec-ch-ua", "\"Not(A:Brand\";v=\"8\", \"Chromium\";v=\"144\", \"Google Chrome\";v=\"144\"")
          .header("sec-ch-ua-mobile", "?0")
          .header("sec-ch-ua-platform", "\"Windows\"")
          .header("sec-fetch-dest", "empty")
          .header("sec-fetch-mode", "cors")
          .header("sec-fetch-site", "same-origin")
          .GET()
          .timeout(apiTimeout)
          .build();
      log.info("downloadOptionChain() sending HTTP GET to URL: {} with timeout {}ms", optionChainUrl, apiTimeoutMs);

      HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
      int code = response.statusCode();
      int bodyLength = response.body() != null ? response.body().length : 0;
      System.out.println("🔗 OPTION CHAIN: HTTP response from " + optionChainUrl + " - status=" + code + ", bodyLength=" + bodyLength);
      log.info("downloadOptionChain() HTTP response from URL {}: status={}, bodyLength={}", optionChainUrl, code, bodyLength);
      
      // Log response headers for debugging
      log.debug("downloadOptionChain() response headers: {}", response.headers().map());
      
      if (code != 200) {
        System.out.println("❌ OPTION CHAIN: HTTP error " + code + " from URL: " + optionChainUrl);
        log.warn("downloadOptionChain() non-200 status from URL {}, returning error", optionChainUrl);
        return "Error: optionChain HTTP " + code + " from URL: " + optionChainUrl;
      }

      String json = new String(response.body(), StandardCharsets.UTF_8);
      Instant downloadTimestamp = Instant.now();
      String timestampStr = downloadTimestamp.toString();
      int dataSizeBytes = json.getBytes(StandardCharsets.UTF_8).length;
      int dataSizeChars = json.length();
      System.out.println("📥 OPTION CHAIN: Downloaded " + dataSizeBytes + " bytes (" + dataSizeChars + " chars) from " + optionChainUrl);
      log.info("📥 DOWNLOADED DATA from URL {}: size={} bytes ({} chars), timestamp={}", optionChainUrl, dataSizeBytes, dataSizeChars, downloadTimestamp);
      log.info("downloadOptionChain() body decoded from URL {}, jsonLength={}, preview={}", optionChainUrl, json.length(), json.length() > 200 ? json.substring(0, 200) + "..." : json);
      
      // Log if response is empty or just {}
      if (json == null || json.trim().isEmpty() || json.trim().equals("{}")) {
        System.out.println("⚠️ OPTION CHAIN: WARNING - Received empty JSON {} from URL: " + optionChainUrl);
        System.out.println("⚠️ OPTION CHAIN: Full response body: '" + json + "'");
        log.warn("⚠️ downloadOptionChain() received empty or empty JSON object from URL: {}", optionChainUrl);
        log.warn("⚠️ Response body: '{}'", json);
        log.warn("⚠️ This may indicate NSE API requires session cookies or different parameters");
      }
      
      String taskName = "optionchain";
      String redisKeyBase = "nse:optionchain:" + symbol + ":" + expiryDate;
      System.out.println("🔗 OPTION CHAIN: Using Redis key base: " + redisKeyBase);
      log.info("downloadOptionChainForExpiry() Redis key base: {}", redisKeyBase);
      publishDataAndNotify(taskName, json, timestampStr, redisKeyBase);

      String okMsg = "OK: optionChain downloaded for " + symbol + " expiry " + expiryDate + ", length=" + json.length() + ", redisKeys=" + redisKeyBase + ":current:data," + redisKeyBase + ":current:timestamp";
      log.info("downloadOptionChainForExpiry() completed successfully: {}", okMsg);
      return okMsg;
    } catch (Exception e) {
      System.out.println("❌ OPTION CHAIN: Failed for expiry " + expiryDate + ": " + e.getMessage());
      log.error("downloadOptionChainForExpiry() failed for expiry {}: {}", expiryDate, e.getMessage(), e);
      return "Error: optionChain for expiry " + expiryDate + " - " + e.getMessage();
    }
  }

  /**
   * Extract list of expiry dates from expiry dates JSON. Returns up to numberOfExpiry dates.
   */
  private List<String> extractExpiryDatesList(String expiryDatesJson, String symbol, int numberOfExpiry) {
    List<String> expiryDates = new ArrayList<>();
    
    if (expiryDatesJson == null || expiryDatesJson.trim().isEmpty() || expiryDatesJson.trim().equals("{}")) {
      System.out.println("⚠️ EXPIRY DATES: No expiry dates JSON available");
      log.warn("extractExpiryDatesList() no expiry dates JSON");
      return expiryDates;
    }
    
    try {
      JsonElement jsonElement = JsonParser.parseString(expiryDatesJson);
      if (!jsonElement.isJsonObject()) {
        log.warn("extractExpiryDatesList() JSON is not an object");
        return expiryDates;
      }
      JsonObject jsonObject = jsonElement.getAsJsonObject();
      
      JsonArray expiryDatesArray = null;
      
      // Try common JSON structures for expiry dates
      if (jsonObject.has("expiryDates") && jsonObject.get("expiryDates").isJsonArray()) {
        expiryDatesArray = jsonObject.getAsJsonArray("expiryDates");
      } else if (jsonObject.has("expiries") && jsonObject.get("expiries").isJsonArray()) {
        expiryDatesArray = jsonObject.getAsJsonArray("expiries");
      } else if (jsonObject.has("data")) {
        JsonObject data = jsonObject.getAsJsonObject("data");
        if (data.has("expiryDates") && data.get("expiryDates").isJsonArray()) {
          expiryDatesArray = data.getAsJsonArray("expiryDates");
        }
      } else {
        // Try first array found in JSON
        for (String key : jsonObject.keySet()) {
          if (jsonObject.get(key).isJsonArray()) {
            expiryDatesArray = jsonObject.getAsJsonArray(key);
            break;
          }
        }
      }
      
      if (expiryDatesArray != null) {
        int count = Math.min(numberOfExpiry, expiryDatesArray.size());
        for (int i = 0; i < count; i++) {
          if (expiryDatesArray.get(i).isJsonPrimitive()) {
            String expiry = expiryDatesArray.get(i).getAsString();
            expiryDates.add(expiry);
          }
        }
        System.out.println("📅 EXPIRY DATES: Extracted " + expiryDates.size() + " expiry dates: " + expiryDates);
        log.info("extractExpiryDatesList() extracted {} expiry dates: {}", expiryDates.size(), expiryDates);
      } else {
        System.out.println("⚠️ EXPIRY DATES: Could not find expiry dates array in JSON");
        log.warn("extractExpiryDatesList() could not find expiry dates array");
      }
    } catch (Exception e) {
      System.out.println("⚠️ EXPIRY DATES: Error parsing expiry dates JSON: " + e.getMessage());
      log.warn("extractExpiryDatesList() error parsing JSON: {}", e.getMessage());
    }
    
    return expiryDates;
  }

  /**
   * Get expiry dates from Redis or fetch fresh from NSE API if not available or needs daily refresh.
   * Refreshes on first call of the day, otherwise uses cached dates from Redis.
   */
  private String getOrRefreshExpiryDates(DownloadNseDataInput input, String symbol) {
    String expiriesDataKey = getExpiriesDataKey(symbol);
    String expiriesTimestampKey = getExpiriesTimestampKey(symbol);
    
    System.out.println("📅 EXPIRY DATES: Checking Redis for cached expiry dates, symbol=" + symbol);
    log.info("getOrRefreshExpiryDates() checking Redis for cached expiry dates, symbol={}, dataKey={}, timestampKey={}", 
        symbol, expiriesDataKey, expiriesTimestampKey);
    
    if (redisPublisher == null) {
      log.warn("getOrRefreshExpiryDates() Redis publisher null, fetching fresh expiry dates");
      return fetchExpiryDatesFromNse(input, symbol);
    }
    
    try {
      String cachedData = redisPublisher.get(expiriesDataKey);
      String cachedTimestamp = redisPublisher.get(expiriesTimestampKey);
      
      if (cachedData != null && cachedTimestamp != null && !cachedData.isEmpty()) {
        // Check if cached data is from today (refresh daily)
        Instant cachedTime = Instant.parse(cachedTimestamp);
        Instant now = Instant.now();
        LocalDate cachedDate = cachedTime.atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate today = now.atZone(ZoneId.systemDefault()).toLocalDate();
        
        if (cachedDate.equals(today)) {
          System.out.println("📅 EXPIRY DATES: Using cached expiry dates from Redis (cached today)");
          log.info("getOrRefreshExpiryDates() using cached expiry dates from Redis, cachedDate={}, today={}", cachedDate, today);
          return cachedData;
        } else {
          System.out.println("📅 EXPIRY DATES: Cached expiry dates are from " + cachedDate + ", refreshing for today");
          log.info("getOrRefreshExpiryDates() cached expiry dates are from {}, refreshing for today {}", cachedDate, today);
        }
      } else {
        System.out.println("📅 EXPIRY DATES: No cached expiry dates found in Redis, fetching fresh");
        log.info("getOrRefreshExpiryDates() no cached expiry dates found in Redis, fetching fresh");
      }
    } catch (Exception e) {
      log.warn("getOrRefreshExpiryDates() error reading from Redis, fetching fresh: {}", e.getMessage());
    }
    
    // Fetch fresh expiry dates and store in Redis
    return fetchExpiryDatesFromNse(input, symbol);
  }

  /**
   * Fetch expiry dates from NSE API and store in Redis.
   */
  private String fetchExpiryDatesFromNse(DownloadNseDataInput input, String symbol) {
    String contractInfoUrl = "https://www.nseindia.com/api/option-chain-contract-info?symbol=" + symbol;
    String expiriesDataKey = getExpiriesDataKey(symbol);
    String expiriesTimestampKey = getExpiriesTimestampKey(symbol);
    System.out.println("📅 EXPIRY DATES: Fetching from NSE API: " + contractInfoUrl);
    log.info("fetchExpiryDatesFromNse() fetching from URL: {}", contractInfoUrl);
    
    try {
      int apiTimeoutMs = input.getTaskTimeout() != null ? input.getTaskTimeout() : 30000;
      Duration apiTimeout = Duration.ofMillis(apiTimeoutMs);
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(contractInfoUrl))
          .header("Accept", "*/*")
          .header("Accept-Language", "en-US,en;q=0.9,hi;q=0.8")
          .header("Referer", NSE_REFERER_OPTIONS)
          .header("User-Agent", USER_AGENT)
          .header("X-Requested-With", "XMLHttpRequest")
          .header("sec-ch-ua", "\"Not(A:Brand\";v=\"8\", \"Chromium\";v=\"144\", \"Google Chrome\";v=\"144\"")
          .header("sec-ch-ua-mobile", "?0")
          .header("sec-ch-ua-platform", "\"Windows\"")
          .header("sec-fetch-dest", "empty")
          .header("sec-fetch-mode", "cors")
          .header("sec-fetch-site", "same-origin")
          .GET()
          .timeout(apiTimeout)
          .build();
      
      HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
      int code = response.statusCode();
      int bodyLength = response.body() != null ? response.body().length : 0;
      System.out.println("📅 EXPIRY DATES: HTTP response from " + contractInfoUrl + " - status=" + code + ", bodyLength=" + bodyLength);
      log.info("fetchExpiryDatesFromNse() HTTP response: status={}, bodyLength={}", code, bodyLength);
      
      if (code != 200) {
        log.warn("fetchExpiryDatesFromNse() non-200 status: {}", code);
        return null;
      }
      
      String expiryJson = new String(response.body(), StandardCharsets.UTF_8);
      Instant fetchTimestamp = Instant.now();
      String timestampStr = fetchTimestamp.toString();
      
      System.out.println("📅 EXPIRY DATES: Fetched " + expiryJson.length() + " chars from NSE API");
      System.out.println("📅 EXPIRY DATES: Preview: " + (expiryJson.length() > 500 ? expiryJson.substring(0, 500) + "..." : expiryJson));
      log.info("fetchExpiryDatesFromNse() fetched expiry dates, length={}, preview={}", 
          expiryJson.length(), expiryJson.length() > 200 ? expiryJson.substring(0, 200) + "..." : expiryJson);
      
      // Log if response is empty
      if (expiryJson == null || expiryJson.trim().isEmpty() || expiryJson.trim().equals("{}")) {
        System.out.println("⚠️ EXPIRY DATES: WARNING - Received empty JSON from contract-info API");
        log.warn("fetchExpiryDatesFromNse() received empty or empty JSON object");
      }
      
      // Store in Redis
      if (redisPublisher != null) {
        redisPublisher.publish(expiriesDataKey, expiryJson);
        redisPublisher.publish(expiriesTimestampKey, timestampStr);
        System.out.println("📅 EXPIRY DATES: Stored in Redis with keys: " + expiriesDataKey + ", " + expiriesTimestampKey);
        log.info("fetchExpiryDatesFromNse() stored expiry dates in Redis: dataKey={}, timestampKey={}", 
            expiriesDataKey, expiriesTimestampKey);
      }
      
      return expiryJson;
    } catch (Exception e) {
      System.out.println("❌ EXPIRY DATES: Failed to fetch: " + e.getMessage());
      log.error("fetchExpiryDatesFromNse() failed: {}", e.getMessage(), e);
      return null;
    }
  }

  /**
   * Extract expiry date from expiry dates JSON. Returns first expiry date found, or "EXPIRY" as fallback.
   */
  private String extractExpiryDate(String expiryDatesJson, String symbol) {
    if (expiryDatesJson == null || expiryDatesJson.trim().isEmpty() || expiryDatesJson.trim().equals("{}")) {
      System.out.println("⚠️ EXPIRY DATE: No expiry dates JSON available, using default 'EXPIRY'");
      log.warn("extractExpiryDate() no expiry dates JSON, using default");
      return "EXPIRY";
    }
    
    try {
      JsonElement jsonElement = JsonParser.parseString(expiryDatesJson);
      if (!jsonElement.isJsonObject()) {
        log.warn("extractExpiryDate() JSON is not an object, using default");
        return "EXPIRY";
      }
      JsonObject jsonObject = jsonElement.getAsJsonObject();
      
      // Try common JSON structures for expiry dates
      if (jsonObject.has("expiryDates") && jsonObject.get("expiryDates").isJsonArray()) {
        JsonArray expiryDates = jsonObject.getAsJsonArray("expiryDates");
        if (expiryDates.size() > 0) {
          String expiry = expiryDates.get(0).getAsString();
          System.out.println("📅 EXPIRY DATE: Extracted from JSON: " + expiry);
          log.info("extractExpiryDate() extracted expiry date: {}", expiry);
          return expiry;
        }
      }
      
      // Try "expiries" array
      if (jsonObject.has("expiries") && jsonObject.get("expiries").isJsonArray()) {
        JsonArray expiries = jsonObject.getAsJsonArray("expiries");
        if (expiries.size() > 0) {
          String expiry = expiries.get(0).getAsString();
          System.out.println("📅 EXPIRY DATE: Extracted from JSON: " + expiry);
          log.info("extractExpiryDate() extracted expiry date: {}", expiry);
          return expiry;
        }
      }
      
      // Try "data" -> "expiryDates"
      if (jsonObject.has("data")) {
        JsonObject data = jsonObject.getAsJsonObject("data");
        if (data.has("expiryDates") && data.get("expiryDates").isJsonArray()) {
          JsonArray expiryDates = data.getAsJsonArray("expiryDates");
          if (expiryDates.size() > 0) {
            String expiry = expiryDates.get(0).getAsString();
            System.out.println("📅 EXPIRY DATE: Extracted from JSON: " + expiry);
            log.info("extractExpiryDate() extracted expiry date: {}", expiry);
            return expiry;
          }
        }
      }
      
      // Try first string value in JSON
      for (String key : jsonObject.keySet()) {
        if (jsonObject.get(key).isJsonArray()) {
          JsonArray arr = jsonObject.getAsJsonArray(key);
          if (arr.size() > 0 && arr.get(0).isJsonPrimitive()) {
            String expiry = arr.get(0).getAsString();
            System.out.println("📅 EXPIRY DATE: Extracted from JSON key '" + key + "': " + expiry);
            log.info("extractExpiryDate() extracted expiry date from key {}: {}", key, expiry);
            return expiry;
          }
        }
      }
      
      System.out.println("⚠️ EXPIRY DATE: Could not parse expiry date from JSON, using default 'EXPIRY'");
      log.warn("extractExpiryDate() could not parse expiry date from JSON structure, using default");
      return "EXPIRY";
    } catch (Exception e) {
      System.out.println("⚠️ EXPIRY DATE: Error parsing expiry dates JSON: " + e.getMessage() + ", using default 'EXPIRY'");
      log.warn("extractExpiryDate() error parsing JSON: {}, using default", e.getMessage());
      return "EXPIRY";
    }
  }

  /**
   * Format expiry date for NSE API URL. Converts various formats to "DD-MMM-YYYY" (e.g., "03-Feb-2026").
   * Handles formats like: "03FEB26", "2026-02-03", "03-Feb-2026", "27JAN26", etc.
   */
  private String formatExpiryForUrl(String expiryDate) {
    if (expiryDate == null || expiryDate.trim().isEmpty()) {
      return "EXPIRY";
    }
    
    String expiry = expiryDate.trim();
    System.out.println("📅 EXPIRY FORMAT: Converting '" + expiry + "' to URL format");
    log.info("formatExpiryForUrl() input: {}", expiry);
    
    try {
      // Try parsing as "DDMMMYY" format (e.g., "03FEB26", "27JAN26")
      if (expiry.length() == 7 && expiry.matches("\\d{2}[A-Z]{3}\\d{2}")) {
        String day = expiry.substring(0, 2);
        String month = expiry.substring(2, 5);
        String year = "20" + expiry.substring(5, 7);
        String formatted = day + "-" + month.substring(0, 1) + month.substring(1, 3).toLowerCase() + "-" + year;
        System.out.println("📅 EXPIRY FORMAT: Parsed as DDMMMYY, formatted: " + formatted);
        log.info("formatExpiryForUrl() parsed DDMMMYY format, result: {}", formatted);
        return formatted;
      }
      
      // Try parsing as "DD-MMM-YYYY" (already correct format)
      if (expiry.matches("\\d{2}-[A-Za-z]{3}-\\d{4}")) {
        // Capitalize first letter of month
        String[] parts = expiry.split("-");
        if (parts.length == 3) {
          String month = parts[1].substring(0, 1).toUpperCase() + parts[1].substring(1).toLowerCase();
          String formatted = parts[0] + "-" + month + "-" + parts[2];
          System.out.println("📅 EXPIRY FORMAT: Already in correct format, normalized: " + formatted);
          log.info("formatExpiryForUrl() normalized existing format, result: {}", formatted);
          return formatted;
        }
      }
      
      // Try parsing as ISO date "YYYY-MM-DD"
      try {
        java.time.LocalDate date = java.time.LocalDate.parse(expiry, DateTimeFormatter.ISO_DATE);
        String formatted = date.format(DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH));
        System.out.println("📅 EXPIRY FORMAT: Parsed as ISO date, formatted: " + formatted);
        log.info("formatExpiryForUrl() parsed ISO date, result: {}", formatted);
        return formatted;
      } catch (DateTimeParseException e) {
        // Try other date formats
      }
      
      // Try parsing as "DD-MMM-YY" (e.g., "03-Feb-26")
      try {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yy", Locale.ENGLISH);
        java.time.LocalDate date = java.time.LocalDate.parse(expiry, formatter);
        String formatted = date.format(DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH));
        System.out.println("📅 EXPIRY FORMAT: Parsed as DD-MMM-YY, formatted: " + formatted);
        log.info("formatExpiryForUrl() parsed DD-MMM-YY format, result: {}", formatted);
        return formatted;
      } catch (DateTimeParseException e) {
        // Continue to fallback
      }
      
      // Fallback: return as-is if it looks like it might already be correct
      System.out.println("📅 EXPIRY FORMAT: Could not parse, using as-is: " + expiry);
      log.warn("formatExpiryForUrl() could not parse expiry date '{}', using as-is", expiry);
      return expiry;
    } catch (Exception e) {
      System.out.println("⚠️ EXPIRY FORMAT: Error formatting expiry date: " + e.getMessage() + ", using as-is: " + expiry);
      log.warn("formatExpiryForUrl() error formatting: {}, using as-is", e.getMessage());
      return expiry;
    }
  }

  /**
   * Common method to publish data to Redis and notify via Kafka.
   * 
   * Redis: Stores data and timestamp separately for current and previous:
   *   - {redisKeyBase}:current:data
   *   - {redisKeyBase}:current:timestamp
   *   - {redisKeyBase}:previous:data (rotated from current)
   *   - {redisKeyBase}:previous:timestamp (rotated from current)
   * 
   * Kafka: Publishes notification with taskName and timestamp to common topic:
   *   - Topic: nse.data (common for all tasks to avoid race conditions)
   *   - Key: {redisKeyBase}:current
   *   - Value: JSON with taskName and timestamp: {"taskName":"...","timestamp":"..."}
   */
  private void publishDataAndNotify(String taskName, String data, String timestamp, String redisKeyBase) {
    String redisKeyCurrent = redisKeyBase + ":current";
    String redisKeyPrevious = redisKeyBase + ":previous";
    
    log.info("publishDataAndNotify() taskName={}, redisKeyBase={}, kafkaTopic={}, dataLength={}", 
        taskName, redisKeyBase, KAFKA_TOPIC_COMMON, data != null ? data.length() : 0);

    // Store full data and timestamp separately in Redis (current and previous)
    if (redisPublisher != null) {
      log.info("publishDataAndNotify() publishing to Redis (rotateAndPublishWithTimestamp)");
      redisPublisher.rotateAndPublishWithTimestamp(redisKeyCurrent, redisKeyPrevious, data, timestamp);
      log.info("publishDataAndNotify() Redis publish done: data stored in {}:data, timestamp in {}:timestamp", 
          redisKeyCurrent, redisKeyCurrent);
    } else {
      log.debug("publishDataAndNotify() Redis publisher null, skipping");
    }

    // Kafka: publish notification with taskName and timestamp to common topic (avoids race conditions)
    if (kafkaPublisher != null) {
      // Create JSON notification: {"taskName":"allIndices","timestamp":"2026-01-28T12:34:56.789Z"}
      String kafkaValue = String.format("{\"taskName\":\"%s\",\"timestamp\":\"%s\"}", taskName, timestamp);
      log.info("publishDataAndNotify() publishing to Kafka topic={} key={} value={}", KAFKA_TOPIC_COMMON, redisKeyCurrent, kafkaValue);
      kafkaPublisher.publish(KAFKA_TOPIC_COMMON, redisKeyCurrent, kafkaValue);
      log.info("publishDataAndNotify() Kafka publish done: notification sent to common topic {} with taskName={}, timestamp={}", KAFKA_TOPIC_COMMON, taskName, timestamp);
    } else {
      log.debug("publishDataAndNotify() Kafka publisher null, skipping");
    }
  }

}
