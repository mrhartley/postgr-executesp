package com.appiancorp.ps.performance;

import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

public class TimedRollingFileAppender extends RollingFileAppender {
  private static final long TIME_UNTIL_FLUSH_MS = 5 * 60 * 1000;
  private long lastFlushed = 0;

  @Override
  protected boolean shouldFlush(LoggingEvent event) {
    if (lastFlushed + TIME_UNTIL_FLUSH_MS < event.getTimeStamp()) {
      lastFlushed = event.getTimeStamp();
      return true;
    }

    return super.shouldFlush(event);
  }
}
