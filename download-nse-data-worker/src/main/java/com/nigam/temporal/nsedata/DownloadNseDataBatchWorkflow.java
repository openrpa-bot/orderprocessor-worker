package com.nigam.temporal.nsedata;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface DownloadNseDataBatchWorkflow {
  @WorkflowMethod(name = "DownloadNseDataBatchWorkflow")
  String downloadNseDataBatch(DownloadNseDataBatchInput batchInput);
}
