package org.apache.calcite.adapter.arrow;

import org.junit.Test;

import java.sql.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Query tests for ArrowTable
 */
public class JdbcTest {

  @Test
  public void all() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/samples/model.json", "admin", "admin")) {
      PreparedStatement pstmt = conn.prepareStatement("select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF");
      ResultSet rs = pstmt.executeQuery();
      resultSetPrint(rs);
    }
  }

  @Test
  public void filter() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/samples/model.json", "admin", "admin")) {
      PreparedStatement pstmt = conn.prepareStatement("select N_REGIONKEY, N_NATIONKEY, N_NAME from NATIONSSF WHERE N_REGIONKEY=?");
      pstmt.setLong(1, 1L);
      ResultSet rs = pstmt.executeQuery();
      resultSetPrint(rs);
    }
  }

  @Test
  public void filterAndGrouping() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/samples/model.json", "admin", "admin")) {
      PreparedStatement pstmt = conn.prepareStatement("select count(*), N_NATIONKEY, N_NAME from NATIONSSF WHERE N_REGIONKEY=? GROUP BY N_NATIONKEY, N_NAME");
      pstmt.setLong(1, 1L);
      ResultSet rs = pstmt.executeQuery();
      resultSetPrint(rs);
    }
  }

  @Test
  public void explain() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/samples/model.json", "admin", "admin")) {
      PreparedStatement pstmt = conn.prepareStatement("explain plan for select N_NATIONKEY, N_NAME from NATIONSSF WHERE N_REGIONKEY=?");
      pstmt.setLong(1, 1L);
      ResultSet rs = pstmt.executeQuery();
      resultSetPrint(rs);
    }
  }

  private static void resultSetPrint(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int count = meta.getColumnCount();
    System.out.println(IntStream.rangeClosed(1, count)
                         .mapToObj(i -> {
                           try {
                             return meta.getColumnLabel(i);
                           } catch (Exception e) {
                             throw new RuntimeException(e);
                           }
                         })
                         .collect(Collectors.joining("\t")));
    while (rs.next()) {
      System.out.println(IntStream.rangeClosed(1, count)
                           .mapToObj(i -> {
                             try {
                               return rs.getObject(i);
                             } catch (Exception e) {
                               throw new RuntimeException(e);
                             }
                           }).map(o -> o == null ? "" : o.toString())
                           .collect(Collectors.joining("\t")));
    }
  }


}
