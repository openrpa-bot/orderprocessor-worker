package com.nigam.temporal.ltp;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface LtpCalculatorWorkflow {
  @WorkflowMethod(name = "LtpCalculatorWorkflow")
  String calculateLtp(LtpCalculatorInput input);
}
