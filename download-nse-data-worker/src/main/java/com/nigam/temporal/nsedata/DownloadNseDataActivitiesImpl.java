package com.nigam.temporal.nsedata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DownloadNseDataActivitiesImpl implements DownloadNseDataActivities {

  private static final Logger log = LogManager.getLogger(DownloadNseDataActivitiesImpl.class);

  private final NseDownloadHandler downloadHandler;

  public DownloadNseDataActivitiesImpl() {
    this(NseDataRedisPublisher.createOrNull(), NseDataKafkaPublisher.createOrNull());
  }

  public DownloadNseDataActivitiesImpl(NseDataRedisPublisher redis, NseDataKafkaPublisher kafka) {
    log.info("DownloadNseDataActivitiesImpl(redis={}, kafka={})", redis != null, kafka != null);
    this.downloadHandler = new NseDownloadHandler(redis, kafka);
    log.info("DownloadNseDataActivitiesImpl NseDownloadHandler created");
  }

  @Override
  public String downloadNseData(DownloadNseDataInput input) {
    System.out.println("📥 DownloadNseData activity STARTED - taskType=" + (input != null ? input.getTaskType() : "null"));
    log.info("downloadNseData() activity invoked, input={}", input);
    if (input == null) {
      System.out.println("❌ DownloadNseData activity ERROR: Input is null");
      log.warn("downloadNseData() input is null, returning error");
      return "Error: Input is null";
    }
    System.out.println("📥 DownloadNseData activity: taskType=" + input.getTaskType() + ", timeout=" + input.getTaskTimeout() + "ms, retries=" + input.getTaskretries());
    log.info("downloadNseData() taskType={}, date={}, targetPath={}, taskTimeout={}ms, taskretries={}, taskdelay={}ms", 
        input.getTaskType(), input.getDate(), input.getTargetPath(), 
        input.getTaskTimeout(), input.getTaskretries(), input.getTaskdelay());
    
    try {
      System.out.println("📥 DownloadNseData activity: calling downloadHandler.handle()");
      log.info("downloadNseData() calling downloadHandler.handle()");
      String result = downloadHandler.handle(input);
      System.out.println("✅ DownloadNseData activity COMPLETED: " + result);
      log.info("downloadNseData() handle() returned: {}", result);
      
      // Apply delay after successful execution
      applyDelay(input.getTaskdelay());
      
      return result;
    } catch (Exception e) {
      log.error("downloadNseData() failed: {}", e.getMessage(), e);
      // Apply delay after failure (before retry if any)
      applyDelay(input.getTaskdelay());
      throw e; // Re-throw to trigger Temporal retry if configured
    }
  }

  /** Apply delay if specified. Called after every execution (success or failure). */
  private void applyDelay(Integer delayMs) {
    if (delayMs != null && delayMs > 0) {
      log.info("downloadNseData() applying delay of {}ms", delayMs);
      try {
        Thread.sleep(delayMs);
        log.debug("downloadNseData() delay completed");
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        log.warn("downloadNseData() delay interrupted");
      }
    }
  }
}
