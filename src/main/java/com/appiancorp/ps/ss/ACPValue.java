package com.appiancorp.ps.ss;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.appiancorp.suiteapi.type.AppianType;
import com.appiancorp.suiteapi.type.Datatype;
import com.appiancorp.suiteapi.type.TypeService;
import com.appiancorp.suiteapi.type.exceptions.InvalidTypeException;
import com.appiancorp.tools.cdth.CDTHelper;
import com.appiancorp.tools.cdth.CDTHelperUtils;

public class ACPValue {
  private static final Logger LOG = Logger.getLogger(ExecuteStoredFunction.class);

  public static Object populate(Long processId, String fullProcedureName, TypeService ts, CallableStatement call, int paramIndex,
    int instanceType) throws SQLException, InvalidTypeException {
    if (instanceType > AppianType.INITIAL_CUSTOM_TYPE) {
      return populate(processId, fullProcedureName, ts, instanceType, (ResultSet) call.getObject(paramIndex));
    }

    switch (instanceType) {
    case AppianType.DOUBLE:
      return call.getDouble(paramIndex);
    case AppianType.DATE:
      return call.getDate(paramIndex);
    case AppianType.TIME:
      return call.getTime(paramIndex);
    case AppianType.TIMESTAMP:
      return call.getTimestamp(paramIndex);
    case AppianType.STRING:
      return call.getString(paramIndex);
    }

    return call.getObject(paramIndex);
  }

  // cdt
  public static Object populate(Long processId, String fullProcedureName, TypeService ts, long multiTypeId, ResultSet rs)
    throws InvalidTypeException, SQLException {
    Datatype multiType = ts.getType(multiTypeId);
    Long singleType = multiType.getTypeof();
    Datatype dtype = ts.getType(singleType);
    List<CDTHelper> helperList = new ArrayList<CDTHelper>();

    ResultSetMetaData meta = rs.getMetaData();
    String[] columnNames = new String[meta.getColumnCount()];

    for (int i = 0; i < columnNames.length; i++) {
      columnNames[i] = meta.getColumnName(i + 1);

      LOG.debug("[" + processId + "] " + fullProcedureName + " rs has column " + columnNames[i] + " with data type " +
        meta.getColumnType(i + 1) + " (" + meta.getColumnTypeName(i + 1) + ")");
    }

    while (rs.next()) {
      CDTHelper helper = CDTHelperUtils.fromNamedTypedValue(dtype.getTypeof(), dtype.getInstanceProperties());

      for (String param : helper.getNames()) {
        for (int i = 0; i < columnNames.length; i++) {
          if (param.equalsIgnoreCase(columnNames[i])) {
            Object value;

            switch (helper.getType(param).intValue()) {
            case AppianType.DOUBLE:
              value = rs.getDouble(columnNames[i]);
              break;
            case AppianType.DATE:
              value = rs.getDate(columnNames[i]);
              break;
            case AppianType.TIME:
              value = rs.getTime(columnNames[i]);
              break;
            case AppianType.TIMESTAMP:
              value = rs.getTimestamp(columnNames[i]);
              break;
            case AppianType.STRING:
              value = rs.getString(columnNames[i]);
              break;
            default:
              value = rs.getObject(columnNames[i]);
              break;
            }

            LOG.debug("[" + processId + "] " + fullProcedureName + " setting rs output " + param + " to " + value);

            helper.setValue(param, value);

            break;
          }
        }
      }

      helperList.add(helper);
    }

    return CDTHelperUtils.getObjectMultiCDT(helperList);
  }
}
