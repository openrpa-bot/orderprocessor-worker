package com.nigam.temporal.nsedata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;

/** Workflow input for NSE download tasks. taskType drives the switch in NseDownloadHandler. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DownloadNseDataInput implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Task type for handler switch: "allIndices", "bhav", "participant", "sec_list", etc. */
  private String taskType;
  private String date;        // yyyyMMdd or similar when applicable
  private String targetPath;  // optional local/store path
  private String symbol;     // Symbol for option chain (e.g., "NIFTY", "BANKNIFTY")
  private Integer numberOfExpiry; // Number of expiry dates to process for option chain (default: 1)
  private Integer taskdelay;  // delay in milliseconds after each call (including first and retry)
  private Integer taskTimeout; // timeout in milliseconds for task execution
  private Integer taskretries; // number of retries on failure (0 = no retry, 1 = retry once)

  public DownloadNseDataInput() {
  }

  public DownloadNseDataInput(String taskType) {
    this.taskType = taskType;
  }

  public DownloadNseDataInput(String taskType, String date) {
    this.taskType = taskType;
    this.date = date;
  }

  public String getTaskType() {
    return taskType;
  }

  public void setTaskType(String taskType) {
    this.taskType = taskType;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getTargetPath() {
    return targetPath;
  }

  public void setTargetPath(String targetPath) {
    this.targetPath = targetPath;
  }

  public Integer getTaskdelay() {
    return taskdelay;
  }

  public void setTaskdelay(Integer taskdelay) {
    this.taskdelay = taskdelay;
  }

  public Integer getTaskTimeout() {
    return taskTimeout;
  }

  public void setTaskTimeout(Integer taskTimeout) {
    this.taskTimeout = taskTimeout;
  }

  public Integer getTaskretries() {
    return taskretries;
  }

  public void setTaskretries(Integer taskretries) {
    this.taskretries = taskretries;
  }

  public String getSymbol() {
    return symbol;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public Integer getNumberOfExpiry() {
    return numberOfExpiry;
  }

  public void setNumberOfExpiry(Integer numberOfExpiry) {
    this.numberOfExpiry = numberOfExpiry;
  }
}
