package com.appiancorp.ps.ss;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.appiancorp.ps.ESPUtils;
import com.appiancorp.suiteapi.security.external.SecureCredentialsStore;
import org.apache.log4j.Logger;

import com.appiancorp.ps.performance.PerformanceTracker;
import com.appiancorp.ps.performance.PerformanceTracker.Metric;
import com.appiancorp.suiteapi.common.Name;
import com.appiancorp.suiteapi.process.ActivityClassParameter;
import com.appiancorp.suiteapi.process.ProcessExecutionService;
import com.appiancorp.suiteapi.process.exceptions.SmartServiceException;
import com.appiancorp.suiteapi.process.framework.AppianSmartService;
import com.appiancorp.suiteapi.process.framework.Input;
import com.appiancorp.suiteapi.process.framework.Order;
import com.appiancorp.suiteapi.process.framework.Required;
import com.appiancorp.suiteapi.process.framework.SmartServiceContext;
import com.appiancorp.suiteapi.process.palette.ConnectivityServices;
import com.appiancorp.suiteapi.type.AppianType;
import com.appiancorp.suiteapi.type.TypeService;

@ConnectivityServices
@Order({ "DataSourceName", "ProcedureName" })
public class ExecuteStoredProcedure extends AppianSmartService {
  private static final Logger LOG = Logger.getLogger(ExecuteStoredProcedure.class);
  private static final int TYPE_ORACLE_CURSOR = -10;

  private SmartServiceContext ctx;
  private ProcessExecutionService pes;
  private TypeService ts;
  private Context initialCtx;
  private Long processId;
  private Connection conn;
  private String dataSourceName;
  private String procedureName;
  private boolean runValidation;
  private boolean pauseOnError;
  private boolean errorOccurred;
  private String errorMessage;
  private ActivityClassParameter[] acps;
  private String searchCatalogName = null;
  private String searchSchemaName = null;
  private String searchProcedureName;
  private String fullProcedureName;
  private List<ProcedureParameter> parameterList = new ArrayList<ProcedureParameter>();
  private HashMap<Integer, ActivityClassParameter> resultSetList = new HashMap<Integer, ActivityClassParameter>();
  private final SecureCredentialsStore scs;


  private static Integer DEFAULT_TIMEOUT_SECS = 14400; //4 hours
  private static final String DEFAULT_PLATFORM_SCS_KEY = "settings.executestoredprocedure"; //to ensure backwards compatibility and to remove the need to refactor existing functions, decided to have a fixed-name scs key for this plug-in
  private static final String DEFAULT_SCS_TIMEOUT_KEY = "timeout";

  public ExecuteStoredProcedure(SmartServiceContext ctx, ProcessExecutionService pes, TypeService ts, Context initialCtx, SecureCredentialsStore scs)
    throws SQLException, NamingException {
    super();

    this.ctx = ctx;
    this.pes = pes;
    this.ts = ts;
    this.initialCtx = initialCtx;
    this.processId = ctx.getProcessProperties().getId();
    this.scs = scs;
  }

  @Override
  public void run() throws SmartServiceException {
    LOG.debug("Execution start");

    PerformanceTracker perf = new PerformanceTracker(
      dataSourceName,
      procedureName,
      ctx.getProcessModelProperties().getId(),
      ctx.getProcessProperties().getId(),
      ctx.getTaskProperties().getName(),
      ctx.getMetadata().isChained());

    try {
      if (searchProcedureName == null || searchProcedureName.isEmpty()) {
        throw new Exception("Procedure name is empty");
      }
      DataSource ds;

      try {
        ds = (DataSource) initialCtx.lookup(dataSourceName);
      } catch (Exception e) {
        throw new Exception("Error looking up datasource " + dataSourceName, e);
      }

      perf.track(Metric.DS_LOOKUP);

      try {
        conn = ds.getConnection();
      } catch (Exception e) {
        throw new Exception("Error obtaining connection " + dataSourceName, e);
      }

      perf.track(Metric.DS_GET_CONNECTION);

      // try {
      fullProcedureName = procedureName;
      acps = ctx.getMetadata().getParameters();
      if (runValidation) {
        LOG.debug("Running SP validation");
        DatabaseMetaData dbMetaData = conn.getMetaData();

        ResultSet procRs = dbMetaData.getProcedures(searchCatalogName, searchSchemaName, searchProcedureName);
        StringBuilder procsAvailableSb = new StringBuilder();
        int procCount = 0;

        while (procRs.next()) {
          String cName = procRs.getString("PROCEDURE_CAT");
          String sName = procRs.getString("PROCEDURE_SCHEM");
          String pName = procRs.getString("PROCEDURE_NAME");

          fullProcedureName = getFullProcedureName(cName, sName, pName);

          if (procCount > 0) {
            procsAvailableSb.append(", ");
          }

          procsAvailableSb.append(fullProcedureName);

          ++procCount;
        }

        procRs.close();

        if (procCount == 0) {
          throw new Exception("No procedure found with the specified name: " +
            getFullProcedureName(searchCatalogName, searchSchemaName, searchProcedureName));
        } else if (procCount > 1) {
          throw new Exception("Many procedures found: " + procCount);
        }

        ResultSet colRs = dbMetaData.getProcedureColumns(searchCatalogName, searchSchemaName, searchProcedureName, null);

        while (colRs.next()) {
          String cName = colRs.getString("PROCEDURE_CAT");
          String sName = colRs.getString("PROCEDURE_SCHEM");
          String pName = colRs.getString("PROCEDURE_NAME");
          short columnType = colRs.getShort("COLUMN_TYPE");
          String columnName = colRs.getString("COLUMN_NAME");
          int dataType = colRs.getInt("DATA_TYPE");
          String typeName = colRs.getString("TYPE_NAME");

          boolean isInput;
          boolean isOutput;
          ActivityClassParameter acp;

          LOG.debug("[" + processId + "] " + getFullProcedureName(cName, sName, pName) + " has column " + columnName + " with data type " +
            dataType + " (" + typeName + ") and column type " + columnType);

          dataType = resolveDataType(dataType, typeName);

          if (columnName == null) {
            continue;
          }

          if (columnName.startsWith("@")) {
            columnName = columnName.substring(1);
          }

          switch (columnType) {
          case DatabaseMetaData.procedureColumnIn:
            isInput = true;
            isOutput = false;
            acp = getACP(columnName);

            if (acp == null) {
              throw new Exception("Missing required input: " + columnName);
            }
            break;
          case DatabaseMetaData.procedureColumnOut:
            isInput = false;
            isOutput = true;
            acp = getACP(columnName);
            break;
          case DatabaseMetaData.procedureColumnInOut:
            isInput = true;
            isOutput = true;
            acp = getACP(columnName);
            break;
          case DatabaseMetaData.procedureColumnReturn:
            // do nothing
            continue;
          default:
            acp = getACP(columnName);
            return;
          }

          if (dataType == TYPE_ORACLE_CURSOR && acp != null) {
            if (acp.getInstanceType() < AppianType.INITIAL_CUSTOM_TYPE) {
              throw new Exception("Invalid type - not a CDT");
            }
          }
          parameterList.add(new ProcedureParameter(columnName, isInput, isOutput, dataType, acp));
        }

        colRs.close();
        LOG.debug("Done validating");
      } else if (!runValidation) {
        for (ActivityClassParameter acp : acps) {
          boolean isInput;
          boolean isOutput;
          String acpName = acp.getName().trim();

          if (acpName.equals("PauseOnError") || acpName.equals("RunValidation") || acpName.equals("DataSourceName") ||
            acpName.equals("ProcedureName") || acpName.toLowerCase().startsWith("resultset")) {
            continue;
          }

          String outputPv = acp.getAssignToProcessVariable();
          Object inputValue = acp.getValue();

          // IN params
          if (outputPv == null || (outputPv.length() == 0)) {
            isInput = true;
            isOutput = false;
            // OUT params
          } else if ((outputPv != null || outputPv.length() != 0) && inputValue != null) {
            isInput = false;
            isOutput = true;
            // INOUT params
          } else {
            isInput = true;
            isOutput = true;
          }
          int dataType = acp.getInstanceType().intValue();
          switch (dataType) {
          case AppianType.INTEGER:
            dataType = Types.INTEGER;
            break;
          case AppianType.DOUBLE:
            dataType = Types.DOUBLE;
            break;
          case AppianType.DATE:
            dataType = Types.DATE;
            break;
          case AppianType.TIME:
            dataType = Types.TIME;
            break;
          case AppianType.TIMESTAMP:
            dataType = Types.TIMESTAMP;
            break;
          case AppianType.STRING:
            dataType = Types.VARCHAR;
            break;
          case AppianType.BOOLEAN:
            dataType = Types.BOOLEAN;
            break;
          }
          parameterList.add(new ProcedureParameter(acp.getName(), isInput, isOutput, dataType, acp));
        }
      }

      // set result set list
      for (ActivityClassParameter acp : acps) {
        if (acp.getName().toLowerCase().startsWith("resultset")) {
          LOG.debug("[" + processId + "] " + fullProcedureName + " found result set input: " + acp.getName());

          if (acp.getInstanceType() < AppianType.INITIAL_CUSTOM_TYPE) {
            throw new Exception("Invalid type - not a CDT");
          }

          try {
            resultSetList.put(
              Integer.parseInt(acp.getName().substring(9)),
              acp);
          } catch (NumberFormatException nfe) {
            LOG.warn("Unexpected value for ResultSet number", nfe);
            throw new Exception("Unexpected value for ResultSet number", nfe);
          }
        }
      }

      perf.track(Metric.VALIDATE);
      conn.setAutoCommit(true);

      CallableStatement call = conn.prepareCall("{call " + procedureName + "(" + getParameters() + ")}");

      Map<String, String> config = scs.getSystemSecuredValues(DEFAULT_PLATFORM_SCS_KEY);

      Integer timeout = ESPUtils.getTimeoutSecs(config.get(DEFAULT_SCS_TIMEOUT_KEY));

      call.setQueryTimeout(timeout);

      for (int i = 0; i < parameterList.size(); i++) {
        int paramIndex = i + 1;
        ProcedureParameter param = parameterList.get(i);

        if (param.isInput()) {
          if (param.getValue() == null) {
            LOG.debug("[" + processId + "] " + fullProcedureName + " setting input " + param.getParameterName() + " as null");
            call.setNull(paramIndex, param.getSQLDataType());
          } else {
            LOG
              .debug("[" + processId + "] " + fullProcedureName + " setting input " + param.getParameterName() + " to " + param.getValue());
            call.setObject(paramIndex, param.getValue());
          }
        }

        if (param.isOutput()) {
          LOG.debug("[" + processId + "] " + fullProcedureName + " registering output " + param.getParameterName());
          call.registerOutParameter(paramIndex, param.getSQLDataType());
        }
      }

      perf.track(Metric.PREPARE);

      boolean hasResultSet = call.execute();

      perf.track(Metric.EXECUTE);

      // set output values
      for (int i = 0; i < parameterList.size(); i++) {
        int paramIndex = i + 1;
        ProcedureParameter param = parameterList.get(i);
        ActivityClassParameter acp = param.getACP();

        if (param.isOutput() && acp != null) {
          int instanceType = acp.getInstanceType().intValue();

          Object resultValue = ACPValue.populate(processId, fullProcedureName, ts, call, paramIndex, instanceType);

          LOG.debug("[" + processId + "] " + fullProcedureName + " setting output " + param.getParameterName() + " to " + resultValue);
          acp.setValue(resultValue);
        }
      }

      // set output result sets
      long totalRowCount = 0;

      if (resultSetList.size() > 0) {
        int currentIndex = 1;

        while (hasResultSet) {
          LOG.debug("[" + processId + "] " + fullProcedureName + " found result set number: " + currentIndex);

          ResultSet rs = call.getResultSet();

          if (resultSetList.containsKey(currentIndex)) {
            ActivityClassParameter acp = resultSetList.get(currentIndex);

            Object resultValue = ACPValue.populate(processId, fullProcedureName, ts, acp.getInstanceType(), rs);

            LOG.debug("[" + processId + "] " + fullProcedureName + " setting rs output " + acp.getName() + " to " + resultValue);
            acp.setValue(resultValue);

            totalRowCount += rs.getRow();
          }

          ++currentIndex;
          hasResultSet = call.getMoreResults();
        }
      }
      call.close();
      LOG.debug("Saving ACPs");
      // save output ACPs
      pes.saveActivityParameters(ctx.getTaskProperties().getId(), ctx.getMetadata().getParameters());

      perf.track(Metric.TRANSFORM);
      perf.trackRowCount(totalRowCount);
    } catch (Exception e) {
      LOG.error("Error during execution", e);

      errorMessage = perf.error(e);
      errorOccurred = true;

      if (pauseOnError) {
        throw new SmartServiceException.Builder(ExecuteStoredProcedure.class, e).build();
      }
    } finally {
      perf.finish();

      try {
        conn.close();
        LOG.debug("Connection closed");
      } catch (Exception e) {
        LOG.error("Error closing connection", e);
      }
    }
  }

  private String getFullProcedureName(String cName, String sName, String pName) {
    String name = pName;

    if (cName != null) {
      name = cName + "." + name;
    }

    if (sName != null) {
      name = sName + "." + name;
    }

    return name;
  }

  private String getParameters() {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < parameterList.size(); i++) {
      if (i == 0) {
        sb.append("?");
      } else {
        sb.append(", ?");
      }
    }

    return sb.toString();
  }

  // Oracle thin driver often returns Types.OTHER for known types, so correct those here.
  private int resolveDataType(int sqlDataType, String sqlTypeName) {
    if (sqlDataType == Types.OTHER) {
      if (sqlTypeName.equals("REF CURSOR")) {
        return TYPE_ORACLE_CURSOR;
      } else if (sqlTypeName.equals("FLOAT")) {
        return Types.FLOAT;
      } else if (sqlTypeName.equals("CLOB")) {
        return Types.CLOB;
      }
    }

    return sqlDataType;
  }

  private ActivityClassParameter getACP(String name) {
    ActivityClassParameter[] acps = ctx.getMetadata().getParameters();
    ActivityClassParameter foundACP = null;
    for (ActivityClassParameter acp : acps) {
      if (acp.getName().equalsIgnoreCase(name)) {
        foundACP = acp;
        break;
      }
    }
    return foundACP;
  }

  @Input(required = Required.ALWAYS)
  @Name("DataSourceName")
  public void setDataSourceName(String val) {
    dataSourceName = val;
  }

  @Input(required = Required.ALWAYS)
  @Name("ProcedureName")
  public void setProcedureName(String val) {
    procedureName = val;
    searchProcedureName = val;

    if (val.contains(".")) {
      String[] names = val.split("\\.");

      if (names.length > 3) {
        // invalid, do nothing
      } else if (names.length > 2) {
        searchCatalogName = names[1];
        searchSchemaName = names[0];
        searchProcedureName = names[2];
      } else if (names.length > 1) {
        searchSchemaName = names[0];
        searchProcedureName = names[1];
      }
    }
  }

  @Input(required = Required.ALWAYS)
  @Name("RunValidation")
  public void setRunValidation(boolean val) {
    runValidation = val;
  }

  @Input(required = Required.ALWAYS)
  @Name("PauseOnError")
  public void setPauseOnError(boolean val) {
    pauseOnError = val;
  }

  @Name("ErrorOccurred")
  public boolean getErrorOccurred() {
    return errorOccurred;
  }

  @Name("ErrorMessage")
  public String getErrorMessage() {
    return errorMessage;
  }
}