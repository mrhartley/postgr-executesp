package com.appiancorp.ps.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.derby.drda.NetworkServerControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import com.appiancorp.plugins.typetransformer.AppianObject;
import com.appiancorp.plugins.typetransformer.AppianTypeFactory;
import com.appiancorp.ps.util.MockServiceLocator;
import com.appiancorp.suiteapi.type.TypeService;
import com.appiancorp.suiteapi.type.TypedValue;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("com.appiancorp.ps.performance.PerformanceTracker")
@PowerMockIgnore("javax.management.*")
public class ExecuteStoredProcedureFunctionTest {
  private NetworkServerControl serverControl;
  private Connection conn;
  private ExecuteStoredProcedureFunction func;
  private InitialContext ctx;
  private TypeService ts;
  private AppianTypeFactory tf;

  @Before
  public void setUp() throws Exception {
    serverControl = new NetworkServerControl(InetAddress.getByName("localhost"), 4444);
    serverControl.start(new PrintWriter(System.out));

    conn = DriverManager.getConnection("jdbc:derby://localhost:4444/memory:test;create=true;");

    try {
      conn.prepareStatement(
        "CREATE PROCEDURE ALL_TYPES(INOUT a SMALLINT, INOUT b INTEGER, INOUT c BIGINT, INOUT d DECIMAL(2, 1), INOUT e REAL, INOUT f DOUBLE, INOUT g FLOAT, INOUT h CHAR(5), INOUT i VARCHAR(10), INOUT j LONG VARCHAR, INOUT k CLOB, INOUT l DATE, INOUT m TIME, INOUT n TIMESTAMP) " +
          "PARAMETER STYLE JAVA " +
          "LANGUAGE JAVA " +
          "READS SQL DATA " +
          "DYNAMIC RESULT SETS 0 " +
          "EXTERNAL NAME " +
          "'com.appiancorp.ps.db.Procedures.allTypes'")
        .executeUpdate();

      conn.prepareStatement(
        "CREATE PROCEDURE TEST_TIMESTAMP(INOUT timestamp TIMESTAMP) " +
          "PARAMETER STYLE JAVA " +
          "LANGUAGE JAVA " +
          "READS SQL DATA " +
          "DYNAMIC RESULT SETS 0 " +
          "EXTERNAL NAME " +
          "'com.appiancorp.ps.db.Procedures.testTimestamp'")
        .executeUpdate();

      conn.prepareStatement(
        "CREATE PROCEDURE NULLABLE_TYPE(OUT a SMALLINT) " +
          "PARAMETER STYLE JAVA " +
          "LANGUAGE JAVA " +
          "READS SQL DATA " +
          "DYNAMIC RESULT SETS 0 " +
          "EXTERNAL NAME " +
          "'com.appiancorp.ps.db.Procedures.nullableType'")
        .executeUpdate();
    } catch (SQLException e) {
      // throw exception if error is not object "already exists"
      if (!e.getMessage().contains("already exists")) {
        throw e;
      }
    }

    func = new ExecuteStoredProcedureFunction();
    ctx = MockServiceLocator.getInitialContext(conn);
    ts = MockServiceLocator.getTypeService();
    tf = AppianTypeFactory.newInstance(ts);
  }

  @Test
  public void testDataTypes() throws SQLException, NamingException {
    long timestamp = System.currentTimeMillis();

    ProcedureInput[] inputs = new ProcedureInput[] {
      new ProcedureInput("A", 1),
      new ProcedureInput("B", 2),
      new ProcedureInput("C", 3),
      new ProcedureInput("D", 4.4),
      new ProcedureInput("E", 5.5),
      new ProcedureInput("F", 6.6),
      new ProcedureInput("G", 7.7),
      new ProcedureInput("H", "hello"),
      new ProcedureInput("I", "i"),
      new ProcedureInput("J", "j"),
      new ProcedureInput("K", "k"),
      new ProcedureInput("L", new Date(timestamp)),
      new ProcedureInput("M", new Time(timestamp)),
      new ProcedureInput("N", new Timestamp(timestamp))
    };

    TypedValue tv = func.executeStoredProcedure(ctx, ts, "jdbc/Test", "ALL_TYPES", inputs);
    AppianObject resp = (AppianObject) tf.toAppianElement(tv);
    AppianObject params = resp.getObject("parameters");

    assertTrue(resp.getValue("success", Boolean.class));
    assertEquals(1, (long) params.getValue("A", Long.class));
    assertEquals(2, (long) params.getValue("B", Long.class));
    assertEquals(3, (long) params.getValue("C", Long.class));
    assertEquals(4.4, (double) params.getValue("D", Double.class), 0.001);
    assertEquals(5.5, (double) params.getValue("E", Double.class), 0.1);
    assertEquals(6.6, (double) params.getValue("F", Double.class), 0.1);
    assertEquals(7.7, (double) params.getValue("G", Double.class), 0.1);
    assertEquals("hello", params.getValue("H", String.class));
    assertEquals("i", params.getValue("I", String.class));
    assertEquals("j", params.getValue("J", String.class));
    assertNull(params.getValue("K", String.class)); // clobs are not supported (too large)
    assertEquals(new Date(timestamp).toString(), params.getValue("L", Date.class).toString());
    assertEquals(new Time(timestamp).toString(), params.getValue("M", Time.class).toString());
    assertEquals(new Timestamp(timestamp).toString(), params.getValue("N", Timestamp.class).toString());
  }

  /*
   * Appian passes timestamps as XMLGregorianCalendar instead of Timestamp's. Test the function handles them ok.
   */
  @Test
  public void testAppianTimestamp() throws SQLException, NamingException, DatatypeConfigurationException {
    XMLGregorianCalendar cal = DatatypeFactory.newInstance().newXMLGregorianCalendar();

    cal.setYear(2017);
    cal.setMonth(1);
    cal.setDay(20);
    cal.setHour(4);
    cal.setMinute(10);
    cal.setSecond(30);
    cal.setTimezone(120); // +2h

    Timestamp expected = new Timestamp(1484878230000L); // GMT-0

    ProcedureInput[] inputs = new ProcedureInput[] {
      new ProcedureInput("TIMESTAMP", cal)
    };

    TypedValue tv = func.executeStoredProcedure(ctx, ts, "jdbc/Test", "TEST_TIMESTAMP", inputs);
    AppianObject resp = (AppianObject) tf.toAppianElement(tv);
    AppianObject params = resp.getObject("parameters");

    assertTrue(resp.getValue("success", Boolean.class));
    assertEquals(expected, params.getValue("TIMESTAMP", Timestamp.class));
  }

  @Test
  public void testNullableType() throws SQLException, NamingException, DatatypeConfigurationException {
    ProcedureInput[] inputs = new ProcedureInput[] {};
    TypedValue tv = func.executeStoredProcedure(ctx, ts, "jdbc/Test", "NULLABLE_TYPE", inputs);
    AppianObject resp = (AppianObject) tf.toAppianElement(tv);
    AppianObject params = resp.getObject("parameters");

    assertTrue(resp.getValue("success", Boolean.class));
    assertNull(params.getValue("A", Integer.class));
  }

  @After
  public void tearDown() throws Exception {
    conn.close();
    serverControl.shutdown();
  }
}
