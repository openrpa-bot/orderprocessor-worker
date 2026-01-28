package com.nigam.temporal.nsedata;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface DownloadNseDataWorkflow {
  @WorkflowMethod(name = "DownloadNseDataWorkflow")
  String downloadNseData(DownloadNseDataInput input);
}
