package com.nigam.temporal.nsedata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Publishes processed NSE data to Kafka. Uses env KAFKA_BOOTSTRAP_SERVERS (default localhost:29092,localhost:29093,localhost:29094).
 * Creates the topic before every publish if it is not available (create-on-use).
 */
public class NseDataKafkaPublisher {

  private static final Logger log = LogManager.getLogger(NseDataKafkaPublisher.class);

  private static final int TOPIC_CREATE_TIMEOUT_SEC = 30;
  private static final int METADATA_WAIT_MS = 3000;
  private static final int METADATA_POLL_MS = 200;

  private final KafkaProducer<String, String> producer;
  private final String bootstrapServers;
  private final Properties adminProps;

  public NseDataKafkaPublisher() {
    bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094");
    log.info("NseDataKafkaPublisher init: bootstrapServers={}", bootstrapServers);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producer = new KafkaProducer<>(props);
    adminProps = new Properties();
    adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    log.info("NseDataKafkaPublisher KafkaProducer created");
  }

  /** Allow null for tests / when Kafka is disabled. */
  public static NseDataKafkaPublisher createOrNull() {
    log.debug("NseDataKafkaPublisher.createOrNull() attempting to create instance");
    try {
      NseDataKafkaPublisher p = new NseDataKafkaPublisher();
      log.info("NseDataKafkaPublisher.createOrNull() created successfully");
      return p;
    } catch (Exception e) {
      log.warn("NseDataKafkaPublisher.createOrNull() failed, returning null: {}", e.getMessage(), e);
      return null;
    }
  }

  /**
   * Create topic if not available. Called before every publish so the topic exists whenever needed.
   * Handles "topic already exists" (e.g. created by another process) and waits for metadata after create.
   */
  private void ensureTopicExists(String topic) {
    try (AdminClient admin = AdminClient.create(adminProps)) {
      Set<String> names = admin.listTopics().names().get(TOPIC_CREATE_TIMEOUT_SEC, TimeUnit.SECONDS);
      if (names != null && names.contains(topic)) {
        log.debug("Kafka topic already exists: {}", topic);
        return;
      }
      log.info("Kafka topic {} not present, creating (partitions=1, replication=1)", topic);
      NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
      CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));
      try {
        result.all().get(TOPIC_CREATE_TIMEOUT_SEC, TimeUnit.SECONDS);
        log.info("Kafka topic created: {}", topic);
      } catch (Exception createEx) {
        if (createEx.getCause() instanceof TopicExistsException
            || (createEx.getMessage() != null && createEx.getMessage().contains("already exists"))) {
          log.info("Kafka topic {} already existed (created elsewhere): {}", topic, createEx.getMessage());
          return;
        }
        throw createEx;
      }
      waitForTopicInMetadata(topic);
    } catch (Exception e) {
      log.warn("Kafka ensureTopicExists({}) failed (will try publish anyway): {}", topic, e.getMessage());
    }
  }

  /** After creating a topic, wait until it appears in producer metadata to avoid "not present in metadata" on first send. */
  private void waitForTopicInMetadata(String topic) {
    long deadline = System.currentTimeMillis() + METADATA_WAIT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (producer.partitionsFor(topic) != null && !producer.partitionsFor(topic).isEmpty()) {
        log.debug("Kafka topic {} now in metadata", topic);
        return;
      }
      try {
        Thread.sleep(METADATA_POLL_MS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        log.warn("Kafka waitForTopicInMetadata interrupted");
        return;
      }
    }
    log.debug("Kafka topic {} metadata wait ended (publish may retry via broker)", topic);
  }

  public void publish(String topic, String key, String value) {
    log.info("Kafka publish() topic={}, key={}, valueLength={}", topic, key, value != null ? value.length() : 0);
    try {
      ensureTopicExists(topic);
      producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
        if (exception != null) {
          log.error("Kafka publish() callback error topic={} key={}: {}", topic, key, exception.getMessage(), exception);
        } else {
          log.info("Kafka publish() callback success topic={} partition={} offset={}", metadata.topic(), metadata.partition(), metadata.offset());
        }
      });
      log.debug("Kafka publish() send() invoked (async)");
    } catch (Exception e) {
      log.error("Kafka publish() failed topic={}: {}", topic, e.getMessage(), e);
      throw e;
    }
  }

  public void close() {
    producer.close();
  }
}
