package org.apache.calcite.adapter.arrow;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.Test;

import java.sql.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

/**
 * Query tests for ArrowTable
 */
public class JdbcTest {

  @Test
  public void nationsAll() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/samples/model.json", "admin", "admin")) {
      PreparedStatement pstmt = conn.prepareStatement("select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF");
      ResultSet rs = pstmt.executeQuery();
      resultSetPrint(rs);
    }
  }

  @Test
  public void regionsAll() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/samples/model.json", "admin", "admin")) {
      PreparedStatement pstmt = conn.prepareStatement("select * from REGIONSSF");
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
  public void filterAndCounting() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/samples/model.json", "admin", "admin")) {
      PreparedStatement pstmt = conn.prepareStatement("select count(*), N_NATIONKEY, N_NAME from NATIONSSF WHERE N_REGIONKEY=? GROUP BY N_NATIONKEY, N_NAME");
      pstmt.setLong(1, 1L);
      ResultSet rs = pstmt.executeQuery();
      resultSetPrint(rs);
    }
  }

  @Test
  public void max() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/samples/model.json", "admin", "admin")) {
      PreparedStatement pstmt = conn.prepareStatement("select N_REGIONKEY, MAX(N_NAME) from NATIONSSF GROUP BY N_REGIONKEY");
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

  private Triple<String, Integer, String>[] joinResults = new Triple[]{
    Triple.of("AFRICA",0,"ALGERIA"),
    Triple.of("AMERICA",1,"ARGENTINA"),
    Triple.of("AMERICA",2,"BRAZIL"),
    Triple.of("AMERICA",3,"CANADA"),
    Triple.of("MIDDLE EAST",4,"EGYPT"),
    Triple.of("AFRICA",5,"ETHIOPIA"),
    Triple.of("EUROPE",6,"FRANCE"),
    Triple.of("EUROPE",7,"GERMANY"),
    Triple.of("ASIA",8,"INDIA"),
    Triple.of("ASIA",9,"INDONESIA"),
    Triple.of("MIDDLE EAST",10,"IRAN"),
    Triple.of("MIDDLE EAST",11,"IRAQ"),
    Triple.of("ASIA",12,"JAPAN"),
    Triple.of("MIDDLE EAST",13,"JORDAN"),
    Triple.of("AFRICA",14,"KENYA"),
    Triple.of("AFRICA",15,"MOROCCO"),
    Triple.of("AFRICA",16,"MOZAMBIQUE"),
    Triple.of("AMERICA",17,"PERU"),
    Triple.of("ASIA",18,"CHINA"),
    Triple.of("EUROPE",19,"ROMANIA"),
    Triple.of("MIDDLE EAST",20,"SAUDI ARABIA"),
    Triple.of("ASIA",21,"VIETNAM"),
    Triple.of("EUROPE",22,"RUSSIA"),
    Triple.of("EUROPE",23,"UNITED KINGDOM"),
    Triple.of("AMERICA",24,"UNITED STATES")
  };

  @Test
  public void innerJoin() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/samples/model.json", "admin", "admin")) {
      PreparedStatement pstmt = conn.prepareStatement("select R.R_NAME, N.N_NATIONKEY, N.N_NAME from NATIONSSF N inner join REGIONSSF R on N.N_REGIONKEY=R.R_REGIONKEY");
      ResultSet rs = pstmt.executeQuery();

      int i = 0;
      while (rs.next()) {
        assertThat(rs.getString("R_NAME"), is(joinResults[i].getLeft()));
        assertThat(rs.getInt("N_NATIONKEY"),  is(joinResults[i].getMiddle()));
        assertThat(rs.getString("N_NAME"),  is(joinResults[i].getRight()));
        i++;
      }
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
