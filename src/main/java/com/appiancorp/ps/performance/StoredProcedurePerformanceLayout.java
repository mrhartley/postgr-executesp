package com.appiancorp.ps.performance;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

public class StoredProcedurePerformanceLayout extends Layout {
  @Override
  public String getHeader() {
    return "Timestamp,Datasource Name,Procedure Name,Datasource Lookup Time (ms),Datasource Connect Time (ms),Validate Time (ms),Prepare Time (ms),Execute Time (ms),Transform Time (ms),Total Time (ms),Error Message,Model Id,Process Id,Is Chained?,Task Name,Total Row Count\r\n";
  }

  @Override
  public String format(LoggingEvent event) {
    StringBuilder sb = new StringBuilder();
    Object[] fields = (Object[]) event.getMessage();

    sb.append(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now()));

    for (Object f : fields) {
      sb.append(",");

      if (f != null) {
        sb.append(StringEscapeUtils.escapeCsv(f.toString()));
      }
    }

    sb.append("\r\n");

    return sb.toString();
  }

  @Override
  public String getContentType() {
    return "text/csv";
  }

  @Override
  public boolean ignoresThrowable() {
    return true;
  }

  @Override
  public void activateOptions() {
    // do nothing
  }
}
