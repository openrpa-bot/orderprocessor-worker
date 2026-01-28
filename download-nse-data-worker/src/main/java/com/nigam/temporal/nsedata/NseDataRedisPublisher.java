package com.nigam.temporal.nsedata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Publishes processed NSE data to Redis. Uses env REDIS_HOST, REDIS_PORT, REDIS_PASSWORD.
 */
public class NseDataRedisPublisher {

  private static final Logger log = LogManager.getLogger(NseDataRedisPublisher.class);

  private final JedisPool pool;
  private final String host;
  private final int port;

  public NseDataRedisPublisher() {
    host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
    port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
    String password = System.getenv().getOrDefault("REDIS_PASSWORD", "");
    log.info("NseDataRedisPublisher init: host={}, port={}, passwordSet={}", host, port, password != null && !password.isEmpty());

    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxTotal(8);
    config.setMaxIdle(4);
    if (password != null && !password.isEmpty()) {
      pool = new JedisPool(config, host, port, 2000, password);
    } else {
      pool = new JedisPool(config, host, port, 2000);
    }
    log.info("NseDataRedisPublisher JedisPool created");
  }

  /** Allow null for tests / when Redis is disabled. */
  public static NseDataRedisPublisher createOrNull() {
    log.debug("NseDataRedisPublisher.createOrNull() attempting to create instance");
    try {
      NseDataRedisPublisher p = new NseDataRedisPublisher();
      log.info("NseDataRedisPublisher.createOrNull() created successfully");
      return p;
    } catch (Exception e) {
      log.warn("NseDataRedisPublisher.createOrNull() failed, returning null: {}", e.getMessage(), e);
      return null;
    }
  }

  public void publish(String key, String value) {
    log.info("Redis publish() key={}, valueLength={}", key, value != null ? value.length() : 0);
    try (Jedis jedis = pool.getResource()) {
      jedis.set(key, value);
      log.info("Redis publish() SET done for key={}", key);
    } catch (Exception e) {
      log.error("Redis publish() failed key={}: {}", key, e.getMessage(), e);
      throw e;
    }
  }

  public String get(String key) {
    log.debug("Redis get() key={}", key);
    try (Jedis jedis = pool.getResource()) {
      String value = jedis.get(key);
      log.debug("Redis get() key={}, valueLength={}", key, value != null ? value.length() : 0);
      return value;
    } catch (Exception e) {
      log.error("Redis get() failed key={}: {}", key, e.getMessage(), e);
      throw e;
    }
  }

  public boolean exists(String key) {
    log.debug("Redis exists() key={}", key);
    try (Jedis jedis = pool.getResource()) {
      boolean exists = jedis.exists(key);
      log.debug("Redis exists() key={}, result={}", key, exists);
      return exists;
    } catch (Exception e) {
      log.error("Redis exists() failed key={}: {}", key, e.getMessage(), e);
      throw e;
    }
  }

  /** Get JedisPool for advanced operations (use with caution, prefer using get/publish methods) */
  public JedisPool getJedisPool() {
    return pool;
  }

  /** Copy current to previous, then set current to value (same pattern as optionchain). */
  public void rotateAndPublish(String currentKey, String previousKey, String value) {
    log.info("Redis rotateAndPublish() currentKey={}, previousKey={}, valueLength={}", currentKey, previousKey, value != null ? value.length() : 0);
    try (Jedis jedis = pool.getResource()) {
      boolean hasCurrent = jedis.exists(currentKey);
      log.debug("Redis rotateAndPublish() exists(currentKey)={}", hasCurrent);
      if (hasCurrent) {
        String existing = jedis.get(currentKey);
        log.debug("Redis rotateAndPublish() copying current->previous, existingLength={}", existing != null ? existing.length() : 0);
        jedis.set(previousKey, existing);
      }
      jedis.set(currentKey, value);
      log.info("Redis rotateAndPublish() done: currentKey set, previousKey rotated={}", hasCurrent);
    } catch (Exception e) {
      log.error("Redis rotateAndPublish() failed: {}", e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Rotate and publish data and timestamp separately.
   * Copies current:data -> previous:data and current:timestamp -> previous:timestamp,
   * then sets new current:data and current:timestamp.
   */
  public void rotateAndPublishWithTimestamp(String baseCurrentKey, String basePreviousKey, String data, String timestamp) {
    String currentDataKey = baseCurrentKey + ":data";
    String currentTimestampKey = baseCurrentKey + ":timestamp";
    String previousDataKey = basePreviousKey + ":data";
    String previousTimestampKey = basePreviousKey + ":timestamp";
    
    log.info("Redis rotateAndPublishWithTimestamp() currentDataKey={}, currentTimestampKey={}, previousDataKey={}, previousTimestampKey={}, dataLength={}", 
        currentDataKey, currentTimestampKey, previousDataKey, previousTimestampKey, data != null ? data.length() : 0);
    try (Jedis jedis = pool.getResource()) {
      boolean hasCurrentData = jedis.exists(currentDataKey);
      boolean hasCurrentTimestamp = jedis.exists(currentTimestampKey);
      log.debug("Redis rotateAndPublishWithTimestamp() exists: currentData={}, currentTimestamp={}", hasCurrentData, hasCurrentTimestamp);
      
      if (hasCurrentData) {
        String existingData = jedis.get(currentDataKey);
        log.debug("Redis rotateAndPublishWithTimestamp() copying current:data->previous:data, existingLength={}", existingData != null ? existingData.length() : 0);
        jedis.set(previousDataKey, existingData);
      }
      if (hasCurrentTimestamp) {
        String existingTimestamp = jedis.get(currentTimestampKey);
        log.debug("Redis rotateAndPublishWithTimestamp() copying current:timestamp->previous:timestamp, existingTimestamp={}", existingTimestamp);
        jedis.set(previousTimestampKey, existingTimestamp);
      }
      
      jedis.set(currentDataKey, data);
      jedis.set(currentTimestampKey, timestamp);
      log.info("Redis rotateAndPublishWithTimestamp() done: current:data and current:timestamp set, previous rotated={}", hasCurrentData || hasCurrentTimestamp);
    } catch (Exception e) {
      log.error("Redis rotateAndPublishWithTimestamp() failed: {}", e.getMessage(), e);
      throw e;
    }
  }
}
