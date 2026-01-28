package com.nigam.temporal.ltp;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface LtpSchedulerWorkflow {
  @WorkflowMethod(name = "LtpSchedulerWorkflow")
  String scheduleLtpCalculations(LtpCalculatorInput input);
}
