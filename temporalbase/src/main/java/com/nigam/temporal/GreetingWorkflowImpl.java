package com.nigam.temporal;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;

public class GreetingWorkflowImpl implements GreetingWorkflow {

  private final GreetingActivities activities =
          Workflow.newActivityStub(
                  GreetingActivities.class,
                  ActivityOptions.newBuilder()
                          .setStartToCloseTimeout(Duration.ofSeconds(10))  // mandatory
                          .setRetryOptions(
                                  RetryOptions.newBuilder()
                                          .setMaximumAttempts(1)
                                          .build()
                          )
                          .build()
          );

  @Override
  public String getGreeting(GreetingInput input) {
    // Extract name from input (handles both string and array formats)
    String nameValue = input != null && input.getName() != null ? input.getName() : "World";
    System.out.println("Called with name: " + nameValue);
    return activities.composeGreeting(nameValue);
  }
}
