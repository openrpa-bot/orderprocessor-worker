package com.nigam.temporal;

public class GreetingActivitiesImpl implements GreetingActivities {
  @Override
  public String composeGreeting(String name) {
    return "Hello, " + name + "!";
  }
}
