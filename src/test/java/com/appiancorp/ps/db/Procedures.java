package com.appiancorp.ps.db;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public class Procedures {
  public static void allTypes(short[] a, int[] b, long[] c, BigDecimal[] d, float[] e, double[] f, double[] g, String[] h, String[] i,
    String[] j, Clob[] k, Date[] l, Time[] m, Timestamp[] n) throws SQLException {

    // no need to do anything, the values passed in will be mapped out as they're INOUT parameters
  }

  public static void testTimestamp(Timestamp[] timestamp) throws SQLException {
    // do nothing, only testing input
  }

  public static void nullableType(Integer[] a) throws SQLException {
    // no need to do anything, we want the value to be null
  }
}
