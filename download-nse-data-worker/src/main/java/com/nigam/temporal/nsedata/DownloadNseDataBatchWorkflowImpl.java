package com.nigam.temporal.nsedata;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class DownloadNseDataBatchWorkflowImpl implements DownloadNseDataBatchWorkflow {

  @Override
  public String downloadNseDataBatch(DownloadNseDataBatchInput batchInput) {
    var logger = Workflow.getLogger(DownloadNseDataBatchWorkflowImpl.class);
    logger.info("downloadNseDataBatch workflow started, taskCount={}", 
        batchInput != null && batchInput.getTasks() != null ? batchInput.getTasks().size() : 0);
    
    if (batchInput == null || batchInput.getTasks() == null || batchInput.getTasks().isEmpty()) {
      logger.warn("downloadNseDataBatch workflow: input is null or tasks list is empty");
      return "Error: Batch input is null or tasks list is empty";
    }

    int interTaskDelayMs = batchInput.getInterTaskDelay() != null ? batchInput.getInterTaskDelay() : 0;
    logger.info("downloadNseDataBatch workflow: totalTasks={}, interTaskDelay={}ms", 
        batchInput.getTasks().size(), interTaskDelayMs);

    List<String> results = new ArrayList<>();
    int taskIndex = 0;

    for (DownloadNseDataInput task : batchInput.getTasks()) {
      taskIndex++;
      logger.info("downloadNseDataBatch workflow: executing task {}/{}: taskType={}", 
          taskIndex, batchInput.getTasks().size(), task.getTaskType());

      if (task == null || task.getTaskType() == null || task.getTaskType().isEmpty()) {
        String errorMsg = String.format("Error: Task %d has null or empty taskType", taskIndex);
        logger.warn("downloadNseDataBatch workflow: {}", errorMsg);
        results.add(errorMsg);
        continue;
      }

      // Use task-specific timeout and retry, or defaults
      int timeoutMs = task.getTaskTimeout() != null ? task.getTaskTimeout() : 600000;
      int retries = task.getTaskretries() != null ? task.getTaskretries() : 0;
      int delayMs = task.getTaskdelay() != null ? task.getTaskdelay() : 0;
      logger.info("downloadNseDataBatch workflow: task {} - timeout={}ms, retries={}, delay={}ms", 
          taskIndex, timeoutMs, retries, delayMs);

      // Build ActivityOptions for this task
      ActivityOptions.Builder optionsBuilder = ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofMillis(timeoutMs));

      if (retries > 0) {
        long retryDelay = delayMs > 0 ? delayMs : 100;
        RetryOptions retryOptions = RetryOptions.newBuilder()
            .setMaximumAttempts(retries + 1)
            .setInitialInterval(Duration.ofMillis(retryDelay))
            .setMaximumInterval(Duration.ofMillis(retryDelay))
            .setBackoffCoefficient(1.0)
            .build();
        optionsBuilder.setRetryOptions(retryOptions);
      }

      DownloadNseDataActivities activities = Workflow.newActivityStub(
          DownloadNseDataActivities.class,
          optionsBuilder.build()
      );

      try {
        String taskResult = activities.downloadNseData(task);
        results.add(String.format("Task %d (%s): %s", taskIndex, task.getTaskType(), taskResult));
        logger.info("downloadNseDataBatch workflow: task {} completed: {}", taskIndex, taskResult);

        // Apply inter-task delay (except after the last task)
        if (taskIndex < batchInput.getTasks().size() && interTaskDelayMs > 0) {
          logger.info("downloadNseDataBatch workflow: applying inter-task delay of {}ms before next task", interTaskDelayMs);
          Workflow.sleep(Duration.ofMillis(interTaskDelayMs));
        }
      } catch (Exception e) {
        String errorMsg = String.format("Task %d (%s): Error - %s", taskIndex, task.getTaskType(), e.getMessage());
        logger.error("downloadNseDataBatch workflow: task {} failed: {}", taskIndex, e.getMessage(), e);
        results.add(errorMsg);
        // Continue with next task even if this one failed
        if (taskIndex < batchInput.getTasks().size() && interTaskDelayMs > 0) {
          Workflow.sleep(Duration.ofMillis(interTaskDelayMs));
        }
      }
    }

    String batchResult = String.join(" | ", results);
    logger.info("downloadNseDataBatch workflow: all tasks completed. Results: {}", batchResult);
    return batchResult;
  }
}
