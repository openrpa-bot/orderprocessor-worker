package com.nigam.temporal;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface GreetingWorkflow {
  @WorkflowMethod(name = "GreetingWorkflow")
  String getGreeting(GreetingInput input);
}
