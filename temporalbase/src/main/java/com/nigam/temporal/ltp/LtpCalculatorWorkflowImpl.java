package com.nigam.temporal.ltp;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;

public class LtpCalculatorWorkflowImpl implements LtpCalculatorWorkflow {

  private final LtpCalculatorActivities activities =
          Workflow.newActivityStub(
                  LtpCalculatorActivities.class,
                  ActivityOptions.newBuilder()
                          .setStartToCloseTimeout(Duration.ofSeconds(30))  // Increased timeout for API calls
                          .setRetryOptions(
                                  RetryOptions.newBuilder()
                                          .setMaximumAttempts(3)  // Retry up to 3 times
                                          .build()
                          )
                          .build()
          );

  @Override
  public String calculateLtp(LtpCalculatorInput input) {
    if (input == null) {
      return "Error: Input is null";
    }
    
    System.out.println("Calculating LTP for:");
    System.out.println("  Server Name: " + input.getServerName());
    System.out.println("  Server IP: " + input.getServerIP());
    System.out.println("  Port: " + input.getPort());
    System.out.println("  Index Name: " + input.getIndexName());
    System.out.println("  Exchange: " + input.getExchange());
    System.out.println("  Expiry: " + input.getExpiry());
    System.out.println("  Strike Range: " + input.getStrikeRange());
    
    // Validate required fields
    if (input.getServerIP() == null || input.getServerIP().isEmpty()) {
      return "Error: ServerIP is required";
    }
    if (input.getPort() == null || input.getPort().isEmpty()) {
      return "Error: Port is required";
    }
    if (input.getApiKey() == null || input.getApiKey().isEmpty()) {
      return "Error: API Key is required";
    }
    if (input.getIndexName() == null || input.getIndexName().isEmpty()) {
      return "Error: Index Name is required";
    }
    if (input.getExchange() == null || input.getExchange().isEmpty()) {
      return "Error: Exchange is required";
    }
    if (input.getExpiry() == null || input.getExpiry().isEmpty()) {
      return "Error: Expiry is required";
    }
    if (input.getStrikeRange() == null || input.getStrikeRange() <= 0) {
      return "Error: Strike Range must be a positive integer";
    }
    
    // Call the activity to fetch option chain
    Integer apiCallPauseMs = input.getApiCallPauseMs() != null ? input.getApiCallPauseMs() : 500;
    String result = activities.fetchOptionChain(
        input.getServerName(),
        input.getServerIP(),
        input.getPort(),
        input.getApiKey(),
        input.getIndexName(),
        input.getExchange(),
        input.getExpiry(),
        input.getStrikeRange(),
        apiCallPauseMs
    );
    
    System.out.println("LTP Calculation Result: " + result);
    return result;
  }
}
