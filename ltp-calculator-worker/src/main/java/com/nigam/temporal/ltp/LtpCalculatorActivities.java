package com.nigam.temporal.ltp;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface LtpCalculatorActivities {
  @ActivityMethod
  String fetchOptionChain(String serverName, String serverIP, String port, String apiKey, String indexName, String exchange, String expiry, Integer strikeRange, Integer apiCallPauseMs);
}
