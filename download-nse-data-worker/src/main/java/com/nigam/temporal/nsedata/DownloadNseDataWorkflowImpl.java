package com.nigam.temporal.nsedata;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;

public class DownloadNseDataWorkflowImpl implements DownloadNseDataWorkflow {

  @Override
  public String downloadNseData(DownloadNseDataInput input) {
    var logger = Workflow.getLogger(DownloadNseDataWorkflowImpl.class);
    logger.info("downloadNseData workflow started, input={}", input != null ? input.getTaskType() : "null");
    if (input == null) {
      logger.warn("downloadNseData workflow: input is null");
      return "Error: Input is null";
    }
    if (input.getTaskType() == null || input.getTaskType().isEmpty()) {
      logger.warn("downloadNseData workflow: taskType missing or empty");
      return "Error: taskType is required";
    }

    // Use dynamic timeout and retry from input, or defaults
    int timeoutMs = input.getTaskTimeout() != null ? input.getTaskTimeout() : 600000; // default 10 minutes
    int retries = input.getTaskretries() != null ? input.getTaskretries() : 0; // default no retries
    int delayMs = input.getTaskdelay() != null ? input.getTaskdelay() : 0; // delay after each call
    logger.info("downloadNseData workflow: taskTimeout={}ms, taskretries={}, taskdelay={}ms", 
        timeoutMs, retries, delayMs);

    // Build ActivityOptions with dynamic timeout and retry policy
    ActivityOptions.Builder optionsBuilder = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofMillis(timeoutMs));

    if (retries > 0) {
      // Use user's delay as retry interval (delay between retry attempts)
      long retryDelay = delayMs > 0 ? delayMs : 100; // use user delay or default 100ms
      RetryOptions retryOptions = RetryOptions.newBuilder()
          .setMaximumAttempts(retries + 1) // retries + 1 = total attempts (1 initial + retries)
          .setInitialInterval(Duration.ofMillis(retryDelay)) // delay before retry
          .setMaximumInterval(Duration.ofMillis(retryDelay)) // same delay (no backoff)
          .setBackoffCoefficient(1.0) // no exponential backoff
          .build();
      optionsBuilder.setRetryOptions(retryOptions);
      logger.info("downloadNseData workflow: retry policy set, maxAttempts={}, retryDelay={}ms", retries + 1, retryDelay);
    }

    DownloadNseDataActivities activities = Workflow.newActivityStub(
        DownloadNseDataActivities.class,
        optionsBuilder.build()
    );

    logger.info("downloadNseData workflow invoking activity for taskType={}", input.getTaskType());
    String result = activities.downloadNseData(input);
    logger.info("downloadNseData workflow activity returned: {}", result);
    return result;
  }
}
