package com.nigam.temporal.nsedata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Batch input for executing multiple NSE download tasks sequentially in a single workflow. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DownloadNseDataBatchInput implements Serializable {
  private static final long serialVersionUID = 1L;

  /** List of tasks to execute sequentially */
  private List<DownloadNseDataInput> tasks;

  /** Delay between tasks in milliseconds (applied after each task completes) */
  private Integer interTaskDelay;

  public DownloadNseDataBatchInput() {
    this.tasks = new ArrayList<>();
  }

  public DownloadNseDataBatchInput(List<DownloadNseDataInput> tasks) {
    this.tasks = tasks != null ? tasks : new ArrayList<>();
  }

  public List<DownloadNseDataInput> getTasks() {
    return tasks;
  }

  public void setTasks(List<DownloadNseDataInput> tasks) {
    this.tasks = tasks != null ? tasks : new ArrayList<>();
  }

  public Integer getInterTaskDelay() {
    return interTaskDelay;
  }

  public void setInterTaskDelay(Integer interTaskDelay) {
    this.interTaskDelay = interTaskDelay;
  }
}
