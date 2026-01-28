package com.nigam.temporal.ltp;

import io.temporal.workflow.Workflow;
import io.temporal.workflow.ChildWorkflowOptions;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class LtpSchedulerWorkflowImpl implements LtpSchedulerWorkflow {

  @Override
  public String scheduleLtpCalculations(LtpCalculatorInput input) {
    if (input == null) {
      return "Error: Input is null";
    }

    // Parse schedule times from input (with defaults)
    String startTimeStr = input.getScheduleStartTime() != null ? input.getScheduleStartTime() : "09:07";
    String endTimeStr = input.getScheduleEndTime() != null ? input.getScheduleEndTime() : "15:30";

    LocalTime START_TIME = parseTime(startTimeStr, LocalTime.of(9, 7));
    LocalTime END_TIME = parseTime(endTimeStr, LocalTime.of(15, 30));

    // Get current time
    LocalTime currentTime = getCurrentTime();

    // Check if current time is within the schedule window
    if (currentTime.isBefore(START_TIME) || currentTime.isAfter(END_TIME) || currentTime.equals(END_TIME)) {
      // Outside schedule window - exit silently
      return "Skipped: Outside schedule window (" + startTimeStr + " - " + endTimeStr + ")";
    }

    // Within schedule window - execute LTP calculation
    System.out.println("⏰ [" + currentTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] Executing LTP calculation");
    System.out.println("   Server: " + input.getServerName());
    System.out.println("   Index: " + input.getIndexName());
    
    try {
      // Start child workflow for LTP calculation
      String workflowId = "ltp-calc-" + UUID.randomUUID().toString();
      
      LtpCalculatorWorkflow childWorkflow = Workflow.newChildWorkflowStub(
          LtpCalculatorWorkflow.class,
          ChildWorkflowOptions.newBuilder()
              .setWorkflowId(workflowId)
              .setTaskQueue("ltpCalculator")
              .build()
      );
      
      String result = childWorkflow.calculateLtp(input);
      System.out.println("✅ Execution completed: " + result);
      return result;
      
    } catch (Exception e) {
      System.err.println("❌ Execution failed: " + e.getMessage());
      e.printStackTrace();
      return "Error: " + e.getMessage();
    }
  }

  private LocalTime getCurrentTime() {
    // In Temporal workflows, we need to use Workflow.currentTimeMillis() for deterministic time
    long currentMillis = Workflow.currentTimeMillis();
    java.time.Instant instant = java.time.Instant.ofEpochMilli(currentMillis);
    java.time.ZoneId zoneId = java.time.ZoneId.systemDefault();
    return instant.atZone(zoneId).toLocalTime();
  }

  private LocalTime parseTime(String timeStr, LocalTime defaultTime) {
    try {
      if (timeStr != null && !timeStr.isEmpty()) {
        return LocalTime.parse(timeStr);
      }
    } catch (Exception e) {
      System.err.println("⚠️ Failed to parse time: " + timeStr + ", using default: " + defaultTime);
    }
    return defaultTime;
  }
}
