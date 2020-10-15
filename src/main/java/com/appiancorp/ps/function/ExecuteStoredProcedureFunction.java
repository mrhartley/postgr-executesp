package com.appiancorp.ps.function;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Savepoint;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;

import com.appiancorp.plugins.typetransformer.AppianList;
import com.appiancorp.plugins.typetransformer.AppianObject;
import com.appiancorp.plugins.typetransformer.AppianTypeFactory;
import com.appiancorp.ps.performance.PerformanceTracker;
import com.appiancorp.ps.performance.PerformanceTracker.Metric;
import com.appiancorp.ps.ss.ProcedureParameter;
import com.appiancorp.suiteapi.expression.annotations.AppianScriptingFunctionsCategory;
import com.appiancorp.suiteapi.expression.annotations.Function;
import com.appiancorp.suiteapi.expression.annotations.Parameter;
import com.appiancorp.suiteapi.security.external.SecureCredentialsStore;
import com.appiancorp.suiteapi.type.AppianType;
import com.appiancorp.suiteapi.type.TypeService;
import com.appiancorp.suiteapi.type.TypedValue;

/**
 * Function that allows stored procedure to be executed from SAIL interfaces
 * 
 * @author hillary.nichols
 * @enhancements betty.huang
 *               - Added new ExecuteStoredFunction to handle PostgreSQL Statement functions where driver needs to be specified in the 3rd
 *               party credentials.
 */
@AppianScriptingFunctionsCategory
public class ExecuteStoredProcedureFunction {
  private static final int TYPE_ORACLE_CURSOR = -10;
  private static final Logger LOG = Logger.getLogger(ExecuteStoredProcedureFunction.class);

  @Function
  public TypedValue executeStoredProcedure(InitialContext ctx, TypeService ts, @Parameter(required = true) String dataSourceName,
    @Parameter(required = true) String procedureName, @Parameter(required = false) ProcedureInput[] inputs) {

    PerformanceTracker perf = new PerformanceTracker(dataSourceName, procedureName);
    AppianTypeFactory tf = AppianTypeFactory.newInstance(ts);
    AppianObject returnValue = (AppianObject) tf.createElement(AppianType.DICTIONARY);
    AppianList emptyResults = null;

    try {
      if (procedureName == null || procedureName.isEmpty()) {
        throw new Exception("The procedure name is either null or empty.");
      }

      if (dataSourceName == null || dataSourceName.isEmpty()) {
        throw new Exception("The data source name is either null or empty.");
      }

      // Retrieve the database connection and validate initial values
      DataSource ds = (DataSource) ctx.lookup(dataSourceName);

      perf.track(Metric.DS_LOOKUP);

      try (Connection conn = ds.getConnection()) {
        return executeStoredProcedure(conn, perf, tf, returnValue, procedureName, inputs);
      }
    } catch (Throwable e) {
      LOG.error("Error executing: ", e);
      returnValue.put("success", tf.createBoolean(false));
      returnValue.put("error", tf.createString(perf.error(e)));
      returnValue.put("parameters", emptyResults);
      returnValue.put("result", emptyResults);
      return tf.toTypedValue(returnValue);
    } finally {
      perf.finish();
    }
  }

  @Function
  public TypedValue executeStoredFunction(SecureCredentialsStore scs, TypeService ts,
    @Parameter(required = true) String scsKey,
    @Parameter(required = true) String procedureName,
    @Parameter(required = false) ProcedureInput[] inputs) {

    PerformanceTracker perf = new PerformanceTracker(scsKey, procedureName);
    AppianTypeFactory tf = AppianTypeFactory.newInstance(ts);
    AppianObject returnValue = (AppianObject) tf.createElement(AppianType.DICTIONARY);
    AppianList emptyResults = null;

    try {
      // Input validation
      if (procedureName == null || procedureName.isEmpty()) {
        throw new Exception("The procedure name is either null or empty.");
      }

      if (scsKey == null || scsKey.isEmpty()) {
        throw new Exception("The secure credential store key is either null or empty.");
      }

      Map<String, String> config = scs.getSystemSecuredValues(scsKey);

      Class.forName(config.get("driver"));

      try (Connection conn = DriverManager.getConnection(config.get("url"), config.get("username"), config.get("password"))) {
        return executeStoredFunction(conn, perf, tf, returnValue, procedureName, inputs);
      }

    } catch (Throwable e) {
      LOG.error("Error executing: ", e);
      returnValue.put("success", tf.createBoolean(false));
      returnValue.put("error", tf.createString(perf.error(e)));
      returnValue.put("parameters", emptyResults);
      returnValue.put("result", emptyResults);
      return tf.toTypedValue(returnValue);
    } finally {
      perf.finish();
    }

  }

  private TypedValue executeStoredFunction(Connection conn, PerformanceTracker perf, AppianTypeFactory tf, AppianObject returnValue,
    String functionName, ProcedureInput[] inputs) throws Exception {
    perf.track(Metric.DS_GET_CONNECTION);

    String fqFunctionName = functionName;

    // Validate that the function can see the stored function and has all mappings
    List<ProcedureParameter> validatedParameters = validate(fqFunctionName, conn, inputs);
    returnValue.put("success", tf.createBoolean(true));
    returnValue.put("error", null);

    perf.track(Metric.VALIDATE);
    conn.setAutoCommit(false);

    String SQL = "SELECT * FROM " + fqFunctionName + " (" + getParameters(validatedParameters) + " )";

    PreparedStatement pstmt = conn.prepareStatement(SQL);

    /* Set parameters */
    setParameters(validatedParameters);

    for (int i = 0; i < validatedParameters.size(); i++) {
      int paramIndex = i + 1;
      ProcedureParameter param = validatedParameters.get(i);

      if (param.isInput()) {
        if (param.getValue() == null) {
          pstmt.setNull(paramIndex, param.getSQLDataType());
        } else {
          if (param.getValue() instanceof XMLGregorianCalendar) {
            XMLGregorianCalendar cal = (XMLGregorianCalendar) param.getValue();

            Timestamp timestamp = new Timestamp(cal.toGregorianCalendar().getTimeInMillis());

            pstmt.setObject(paramIndex, timestamp);
          } else {
            pstmt.setObject(paramIndex, param.getValue());
          }
        }
      }

      if (param.isOutput()) {
        LOG.debug(functionName + " registering output " + param.getParameterName());
        ((CallableStatement) pstmt).registerOutParameter(paramIndex, param.getSQLDataType());
      }
    }

    perf.track(Metric.PREPARE);

    // execution
    boolean hasResultSet = pstmt.execute();

    perf.track(Metric.EXECUTE);

    // parameters
    AppianObject parameters = (AppianObject) tf.createElement(AppianType.DICTIONARY);
    long totalRowCount = 0;
    int pos = 1;

    for (ProcedureParameter param : validatedParameters) {
      if (param.isOutput()) {
        if (((CallableStatement) pstmt).getObject(pos) == null) {
          parameters.put(param.getParameterName(), null);
          continue;
        }

        switch (param.getSQLDataType()) {
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.NCHAR:
        case Types.CHAR:
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
          parameters.put(param.getParameterName(), tf.createString(((CallableStatement) pstmt).getString(pos)));
          break;
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.NUMERIC:
        case Types.REAL:
          parameters.put(param.getParameterName(), tf.createDouble(((CallableStatement) pstmt).getDouble(pos)));
          break;
        case Types.DATE:
          parameters.put(param.getParameterName(), tf.createDate(((CallableStatement) pstmt).getDate(pos)));
          break;
        case Types.TIME:
          parameters.put(param.getParameterName(), tf.createTime(((CallableStatement) pstmt).getTime(pos)));
          break;
        case Types.TIMESTAMP:
          parameters.put(param.getParameterName(), tf.createDateTime(((CallableStatement) pstmt).getTimestamp(pos)));
          break;
        case Types.BIGINT:
        case Types.INTEGER:
        case Types.SMALLINT:
        case Types.TINYINT:
          parameters.put(param.getParameterName(), tf.createLong(((CallableStatement) pstmt).getLong(pos)));
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          parameters.put(param.getParameterName(), tf.createBoolean(((CallableStatement) pstmt).getBoolean(pos)));
          break;
        default:
          LOG.debug("The datatype " + param.getSQLDataType() + " for output parameter " + param.getParameterName() +
            " + is not supported.");
        }
      }
      pos++;
    }

    returnValue.put("parameters", parameters);

    // results
    AppianList resultList = tf.createList(AppianType.VARIANT);

    if (hasResultSet) {
      AppianList list = getProcedureResultSet(pstmt, tf, functionName);

      totalRowCount += list.size();

      resultList.add(list);
    }

    returnValue.put("result", resultList);

    // Commit the save point
    conn.commit();

    TypedValue ret = tf.toTypedValue(returnValue);

    perf.track(Metric.TRANSFORM);
    perf.trackRowCount(totalRowCount);

    return ret;

  }

  private void setParameters(List<ProcedureParameter> parameterList) {

    for (int i = 0; i < parameterList.size(); i++) {
      parameterList.get(i);
    }
  }

  private TypedValue executeStoredProcedure(Connection conn, PerformanceTracker perf, AppianTypeFactory tf, AppianObject returnValue,
    String procedureName, ProcedureInput[] inputs) throws Exception {
    perf.track(Metric.DS_GET_CONNECTION);

    // Validate that the function can see the stored procedure and has all mappings
    List<ProcedureParameter> parameterList = validate(procedureName, conn, inputs);
    returnValue.put("success", tf.createBoolean(true));
    returnValue.put("error", tf.createString(""));

    perf.track(Metric.VALIDATE);
    conn.setAutoCommit(false);

    // Start a transaction save point
    Savepoint save = conn.setSavepoint();

    // create a CallableStatement, set the inputs/outputs
    CallableStatement call = conn.prepareCall("{call " + procedureName + "(" + getParameters(parameterList) + ")}");

    for (int i = 0; i < parameterList.size(); i++) {
      int paramIndex = i + 1;
      ProcedureParameter param = parameterList.get(i);

      if (param.isInput()) {
        if (param.getValue() == null) {
          LOG.debug(procedureName + " setting input " + param.getParameterName() + " as null");
          call.setNull(paramIndex, param.getSQLDataType());
        } else {
          LOG.debug(procedureName + " setting input " + param.getParameterName() + " to " + param.getValue());
          if (param.getValue() instanceof XMLGregorianCalendar) {
            XMLGregorianCalendar cal = (XMLGregorianCalendar) param.getValue();

            Timestamp timestamp = new Timestamp(cal.toGregorianCalendar().getTimeInMillis());

            call.setObject(paramIndex, timestamp);
          } else {
            call.setObject(paramIndex, param.getValue());
          }
        }
      }

      if (param.isOutput()) {
        LOG.debug(procedureName + " registering output " + param.getParameterName());
        call.registerOutParameter(paramIndex, param.getSQLDataType());
      }
    }

    perf.track(Metric.PREPARE);

    // execution
    boolean hasResultSet = call.execute();

    perf.track(Metric.EXECUTE);

    // parameters
    AppianObject parameters = (AppianObject) tf.createElement(AppianType.DICTIONARY);
    long totalRowCount = 0;
    int pos = 1;

    for (ProcedureParameter param : parameterList) {
      if (param.isOutput()) {
        if (call.getObject(pos) == null) {
          parameters.put(param.getParameterName(), null);
          continue;
        }

        switch (param.getSQLDataType()) {
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.NCHAR:
        case Types.CHAR:
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
          parameters.put(param.getParameterName(), tf.createString(call.getString(pos)));
          break;
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.NUMERIC:
        case Types.REAL:
          parameters.put(param.getParameterName(), tf.createDouble(call.getDouble(pos)));
          break;
        case Types.DATE:
          parameters.put(param.getParameterName(), tf.createDate(call.getDate(pos)));
          break;
        case Types.TIME:
          parameters.put(param.getParameterName(), tf.createTime(call.getTime(pos)));
          break;
        case Types.TIMESTAMP:
          parameters.put(param.getParameterName(), tf.createDateTime(call.getTimestamp(pos)));
          break;
        case Types.BIGINT:
        case Types.INTEGER:
        case Types.SMALLINT:
        case Types.TINYINT:
          parameters.put(param.getParameterName(), tf.createLong(call.getLong(pos)));
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          parameters.put(param.getParameterName(), tf.createBoolean(call.getBoolean(pos)));
          break;
        case TYPE_ORACLE_CURSOR:
          AppianList resultset = getResultSet((ResultSet) call.getObject(pos), tf, procedureName);

          totalRowCount += resultset.size();

          parameters.put(param.getParameterName(), resultset);
          break;
        default:
          LOG.debug("The datatype " + param.getSQLDataType() + " for output parameter " + param.getParameterName() +
            " + is not supported.");
        }
      }
      pos++;
    }

    returnValue.put("parameters", parameters);

    // results
    AppianList resultList = tf.createList(AppianType.VARIANT);

    if (hasResultSet) {
      AppianList list = getProcedureResultSet(call, tf, procedureName);

      totalRowCount += list.size();

      resultList.add(list);
    }

    // Loop through results (make sure user isn't writing data, rollback if so)
    while (true) {
      if (call.getMoreResults()) {
        AppianList list = getProcedureResultSet(call, tf, procedureName);

        totalRowCount += list.size();

        resultList.add(list);
      } else if (call.getUpdateCount() > 0) {
        conn.rollback(save);

        throw new Exception(
          "Use the Execute Stored Procedure Smart Service to modify data. This function must only be used to query data, not modify.");
      } else if (call.getUpdateCount() < 0) {
        // nothing else to process
        break;
      }
    }

    returnValue.put("result", resultList);

    // Commit the save point
    conn.commit();

    TypedValue ret = tf.toTypedValue(returnValue);

    perf.track(Metric.TRANSFORM);
    perf.trackRowCount(totalRowCount);

    return ret;
  }

  private List<ProcedureParameter> validate(String procedureName, Connection conn, ProcedureInput[] inputs)
    throws Exception {
    List<ProcedureParameter> parameterList = new ArrayList<>();
    DatabaseMetaData dbMetaData = conn.getMetaData();
    String searchCatalogName = null;
    String searchSchemaName = null;
    String searchProcedureName = procedureName;

    if (procedureName.contains(".")) {
      String[] names = procedureName.split("\\.");

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

    StringBuilder procsAvailableSb = new StringBuilder();
    int procCount = 0;

    try (ResultSet procRs = dbMetaData.getProcedures(searchCatalogName, searchSchemaName, searchProcedureName)) {
      while (procRs.next()) {
        String cName = procRs.getString("PROCEDURE_CAT");
        String sName = procRs.getString("PROCEDURE_SCHEM");
        String pName = procRs.getString("PROCEDURE_NAME");

        String fullProcedureName = getFullProcedureName(cName, sName, pName);

        if (procCount > 0) {
          procsAvailableSb.append(", ");
        }

        procsAvailableSb.append(fullProcedureName);

        ++procCount;
      }
    }

    if (procCount == 0) {
      LOG.error("No procedure found with name: " +
        getFullProcedureName(searchCatalogName, searchSchemaName, searchProcedureName) + ".");
      throw new Exception("No procedure found with name: " +
        getFullProcedureName(searchCatalogName, searchSchemaName, searchProcedureName) + ".");
    } else if (procCount > 1) {
      LOG.error("Multiple procedures found with name: " + getFullProcedureName(searchCatalogName, searchSchemaName, searchProcedureName) +
        ": " + procsAvailableSb);
      throw new Exception("Multiple procedures found with name: " +
        getFullProcedureName(searchCatalogName, searchSchemaName, searchProcedureName) + ": " + procsAvailableSb);
    }

    try (ResultSet colRs = dbMetaData.getProcedureColumns(searchCatalogName, searchSchemaName, searchProcedureName, null)) {
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
        ProcedureInput procedureInput;

        LOG.debug(getFullProcedureName(cName, sName, pName) + " has column " + columnName + " with data type " + dataType + " (" +
          typeName + ") and column type " + columnType);

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
          procedureInput = getMatchingProcedureInput(inputs, columnName);
          break;
        case DatabaseMetaData.procedureColumnOut:
          isInput = false;
          isOutput = true;
          procedureInput = getMatchingProcedureInput(inputs, columnName);
          break;
        case DatabaseMetaData.procedureColumnInOut:
          isInput = true;
          isOutput = true;
          procedureInput = getMatchingProcedureInput(inputs, columnName);
          break;
        case DatabaseMetaData.procedureColumnReturn:
          // do nothing
          continue;
        default:
          LOG.error("Unsupported input for the procedure. Column type: " + columnType + ". Column name: " + columnName);
          throw new Exception("Unsupported input for the procedure. Column type: " + columnType + ". Column name: " + columnName);
        }

        parameterList.add(new ProcedureParameter(columnName, isInput, isOutput, dataType, procedureInput));
      }
    }

    return parameterList;
  }

  private ProcedureInput getMatchingProcedureInput(ProcedureInput[] inputs, String columnName) {
    for (int i = 0; i < inputs.length; ++i) {
      if (inputs[i].getName().equals(columnName)) {
        return inputs[i];
      }
    }
    return null;
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

  private AppianList getProcedureResultSet(CallableStatement call, AppianTypeFactory tf, String fullProcedureName)
    throws Exception {
    return getResultSet(call.getResultSet(), tf, fullProcedureName);
  }

  private AppianList getProcedureResultSet(PreparedStatement call, AppianTypeFactory tf, String fullProcedureName)
    throws Exception {
    return getResultSet(call.getResultSet(), tf, fullProcedureName);
  }

  private AppianList getResultSet(ResultSet rsin, AppianTypeFactory tf, String fullProcedureName) throws Exception {
    try (ResultSet rs = rsin) {
      ResultSetMetaData meta;
      meta = rs.getMetaData();
      Map<String, Integer> columnTypes = new HashMap<>();

      for (int i = 0; i < meta.getColumnCount(); i++) {
        columnTypes.put(meta.getColumnName(i + 1), meta.getColumnType(i + 1));

        LOG.debug(fullProcedureName + " rs has column " + meta.getColumnName(i + 1) + " with data type " + meta.getColumnType(i + 1) +
          " (" + meta.getColumnTypeName(i + 1) + ")");

      }
      AppianList resultset = tf.createList(AppianType.LIST_OF_DICTIONARY);
      while (rs.next()) {
        AppianObject row = (AppianObject) tf.createElement(AppianType.DICTIONARY);
        for (Entry<String, Integer> col : columnTypes.entrySet()) {
          if (rs.getObject(col.getKey()) == null) {
            row.put(col.getKey(), null);
            continue;
          }

          switch (col.getValue()) {
          case Types.VARCHAR:
          case Types.NVARCHAR:
          case Types.NCHAR:
          case Types.CHAR:
          case Types.LONGNVARCHAR:
          case Types.LONGVARCHAR:
            row.put(col.getKey(), tf.createString(rs.getString(col.getKey())));
            break;
          case Types.DECIMAL:
          case Types.DOUBLE:
          case Types.FLOAT:
          case Types.NUMERIC:
          case Types.REAL:
            row.put(col.getKey(), tf.createDouble(rs.getDouble(col.getKey())));
            break;
          case Types.DATE:
            row.put(col.getKey(), tf.createDate(rs.getDate(col.getKey())));
            break;
          case Types.TIME:
            row.put(col.getKey(), tf.createTime(rs.getTime(col.getKey())));
            break;
          case Types.TIMESTAMP:
            row.put(col.getKey(), tf.createDateTime(rs.getTimestamp(col.getKey())));
            break;
          case Types.BIGINT:
          case Types.INTEGER:
          case Types.SMALLINT:
          case Types.TINYINT:
            row.put(col.getKey(), tf.createLong(rs.getLong(col.getKey())));
            break;
          case Types.BIT:
          case Types.BOOLEAN:
            row.put(col.getKey(), tf.createBoolean(rs.getBoolean(col.getKey())));
            break;
          default:
            LOG.debug("The datatype " + col.getValue() + " for " + col.getKey() + " + is not supported.");
          }
        }
        resultset.add(row);
      }
      return resultset;
    } catch (Exception e) {
      throw new Exception("Error processing results in procedure " + fullProcedureName);
    }
  }

  private String getParameters(List<ProcedureParameter> parameterList) {
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
}