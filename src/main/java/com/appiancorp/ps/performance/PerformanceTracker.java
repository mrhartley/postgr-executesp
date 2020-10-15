package com.appiancorp.ps.performance;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.appiancorp.suiteapi.cfg.ConfigurationLoader;

public class PerformanceTracker {
  private static final Logger LOG = Logger.getLogger(PerformanceTracker.class);

  public enum Metric {
    DS_LOOKUP, DS_GET_CONNECTION, VALIDATE, PREPARE, EXECUTE, TRANSFORM, ROW_COUNT
  }

  static {
    TimedRollingFileAppender rfa = new TimedRollingFileAppender();

    rfa.setName(PerformanceTracker.class.getName());
    rfa.setFile(ConfigurationLoader.getConfiguration().getAeLogs() + "/perflogs/execute_stored_procedure_trace.csv");
    rfa.setLayout(new StoredProcedurePerformanceLayout());
    rfa.setThreshold(Level.INFO);
    rfa.setAppend(true);
    rfa.setMaxFileSize("10MB");
    rfa.setMaxBackupIndex(1000);
    rfa.setBufferedIO(true);
    rfa.setBufferSize(2048);
    rfa.activateOptions();

    LOG.removeAllAppenders();
    LOG.addAppender(rfa);
    LOG.setAdditivity(false);
    LOG.setLevel(Level.INFO);
  }

  private Map<Metric, Long> perfMetrics = new HashMap<>();
  private Long rowCount;
  private Long previous;
  private String dataSourceName;
  private String procedureName;
  private Long pmId;
  private Long processId;
  private Boolean chained;
  private String taskName;
  private long totalTime = 0;
  private String errorMessage;

  public PerformanceTracker(String dataSourceName, String procedureName) {
    this.previous = System.currentTimeMillis();
    this.dataSourceName = dataSourceName;
    this.procedureName = procedureName;
  }

  public PerformanceTracker(
    String dataSourceName, String procedureName, long pmId, long processId,
    String taskName, boolean chained) {
    this(dataSourceName, procedureName);
    this.pmId = pmId;
    this.processId = processId;
    this.taskName = taskName;
    this.chained = chained;
  }

  public void track(Metric metricCompletion) {
    long now = System.currentTimeMillis();
    long time = now - previous;
    this.previous = now;
    this.totalTime += time;
    this.perfMetrics.put(metricCompletion, time);
  }

  public void trackRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  public String error(Throwable t) {
    return this.errorMessage = t.getMessage() == null ? StringUtils.abbreviate(ExceptionUtils.getStackTrace(t), 500) : t.getMessage();
  }

  public void finish() {
    if (LOG != null) {
      LOG.log(Level.INFO,
        new Object[] {
          dataSourceName, procedureName, perfMetrics.get(Metric.DS_LOOKUP), perfMetrics.get(Metric.DS_GET_CONNECTION),
          perfMetrics.get(Metric.VALIDATE), perfMetrics.get(Metric.PREPARE), perfMetrics.get(Metric.EXECUTE),
          perfMetrics.get(Metric.TRANSFORM), totalTime, errorMessage, pmId, processId, chained, taskName, rowCount
        });
    }
  }
}
