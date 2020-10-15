package com.appiancorp.ps.util;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.appiancorp.suiteapi.type.AppianType;
import com.appiancorp.suiteapi.type.Datatype;
import com.appiancorp.suiteapi.type.TypeService;

public class MockServiceLocator {
  public static final TypeService getTypeService() {
    TypeService ts = mock(TypeService.class);

    registerType(ts, AppianType.BOOLEAN, AppianType.LIST_OF_BOOLEAN);
    registerType(ts, AppianType.DATE, AppianType.LIST_OF_DATE);
    registerType(ts, AppianType.DEFERRED, AppianType.LIST_OF_DEFERRED);
    registerType(ts, AppianType.DOUBLE, AppianType.LIST_OF_DOUBLE);
    registerType(ts, AppianType.DICTIONARY, AppianType.LIST_OF_DICTIONARY);
    registerType(ts, AppianType.INTEGER, AppianType.LIST_OF_INTEGER);
    registerType(ts, AppianType.STRING, AppianType.LIST_OF_STRING);
    registerType(ts, AppianType.TIME, AppianType.LIST_OF_TIME);
    registerType(ts, AppianType.TIMESTAMP, AppianType.LIST_OF_TIMESTAMP);
    registerType(ts, AppianType.VARIANT, AppianType.LIST_OF_VARIANT);

    return ts;
  }

  private static final void registerType(TypeService ts, long singleTypeId, long listTypeId) {
    // single
    Datatype dType = mock(Datatype.class);

    when(dType.getId()).thenReturn(singleTypeId);
    when(dType.isListType()).thenReturn(false);
    when(dType.isRecordType()).thenReturn(false);
    when(dType.getList()).thenReturn(listTypeId);

    // list
    Datatype dTypeList = mock(Datatype.class);
    when(dTypeList.getId()).thenReturn(listTypeId);
    when(dTypeList.isListType()).thenReturn(true);
    when(dTypeList.isRecordType()).thenReturn(false);
    when(dTypeList.getList()).thenReturn(listTypeId);

    when(ts.getType(singleTypeId)).thenReturn(dType);
    when(ts.getType(listTypeId)).thenReturn(dTypeList);
  }

  public static InitialContext getInitialContext(Connection conn) throws NamingException, SQLException {
    InitialContext ctx = mock(InitialContext.class);
    DataSource ds = mock(DataSource.class);

    when(ctx.lookup(anyString())).thenReturn(ds);
    when(ds.getConnection()).thenReturn(conn);

    return ctx;
  }
}
