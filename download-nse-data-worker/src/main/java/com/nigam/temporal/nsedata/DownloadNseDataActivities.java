package com.nigam.temporal.nsedata;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface DownloadNseDataActivities {
  @ActivityMethod
  String downloadNseData(DownloadNseDataInput input);
}
