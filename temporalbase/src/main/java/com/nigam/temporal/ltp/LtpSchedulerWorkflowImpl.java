package com.nigam.temporal.ltp;

import io.temporal.workflow.Workflow;
import io.temporal.workflow.ChildWorkflowOptions;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class LtpSchedulerWorkflowImpl implements LtpSchedulerWorkflow {

  private static final LocalTime START_TIME = LocalTime.of(9, 7);  // 9:07 AM
  private static final LocalTime END_TIME = LocalTime.of(15, 30);  // 3:30 PM
  private static final Duration INTERVAL = Duration.ofMinutes(1);   // 1 minute

  @Override
  public String scheduleLtpCalculations(LtpCalculatorInput input) {
    if (input == null) {
      return "Error: Input is null";
    }

    System.out.println("🕐 Starting LTP Scheduler Workflow");
    System.out.println("   Schedule: Every minute from 9:07 AM to 3:30 PM");
    System.out.println("   Server: " + input.getServerName());
    System.out.println("   Index: " + input.getIndexName());

    int executionCount = 0;
    LocalTime currentTime;

    // Wait until 9:07 AM if we're starting before that
    currentTime = getCurrentTime();
    if (currentTime.isBefore(START_TIME)) {
      Duration waitTime = Duration.between(currentTime, START_TIME);
      System.out.println("⏳ Waiting until 9:07 AM... (" + waitTime.toMinutes() + " minutes)");
      Workflow.sleep(waitTime);
    }

    // Main scheduling loop
    while (true) {
      currentTime = getCurrentTime();
      
      // Check if we've passed 3:30 PM
      if (currentTime.isAfter(END_TIME) || currentTime.equals(END_TIME)) {
        System.out.println("🛑 Reached end time (3:30 PM). Stopping scheduler.");
        break;
      }

      // Execute LTP calculation
      executionCount++;
      System.out.println("⏰ [" + currentTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] Execution #" + executionCount);
      
      try {
        // Start child workflow for LTP calculation
        String workflowId = "ltp-calc-" + UUID.randomUUID().toString();
        
        // Use child workflow to execute the calculation
        LtpCalculatorWorkflow childWorkflow = Workflow.newChildWorkflowStub(
            LtpCalculatorWorkflow.class,
            ChildWorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue("ltpCalculator")
                .build()
        );
        
        String result = childWorkflow.calculateLtp(input);
        System.out.println("✅ Execution #" + executionCount + " completed: " + result);
        
      } catch (Exception e) {
        System.err.println("❌ Execution #" + executionCount + " failed: " + e.getMessage());
        e.printStackTrace();
      }

      // Sleep for 1 minute before next execution
      Workflow.sleep(INTERVAL);
    }

    String summary = String.format("Scheduler completed. Total executions: %d", executionCount);
    System.out.println("📊 " + summary);
    return summary;
  }

  private LocalTime getCurrentTime() {
    // In Temporal workflows, we need to use Workflow.currentTimeMillis() for deterministic time
    long currentMillis = Workflow.currentTimeMillis();
    java.time.Instant instant = java.time.Instant.ofEpochMilli(currentMillis);
    java.time.ZoneId zoneId = java.time.ZoneId.systemDefault();
    return instant.atZone(zoneId).toLocalTime();
  }
}
